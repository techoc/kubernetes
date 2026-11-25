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
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	eventv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/kubernetes/pkg/controller/apis/config/scheme"
	"k8s.io/kubernetes/pkg/controller/garbagecollector/metaonly"
)

// eventType 表示图变更事件的类型。
type eventType int

// String 返回事件类型的可读名称。
func (e eventType) String() string {
	switch e {
	case addEvent:
		return "add"
	case updateEvent:
		return "update"
	case deleteEvent:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

// 图变更事件类型的枚举值。
const (
	addEvent eventType = iota
	updateEvent
	deleteEvent
)

// event 封装 informer 传来或人工构造的图变更事件。
type event struct {
	// virtual 表示该事件并非来自 informer，而是人工构造的（如虚拟删除事件）。
	virtual   bool
	eventType eventType
	obj       interface{}
	// 更新事件会带上旧对象，但垃圾回收器并不使用它。
	oldObj interface{}
	gvk    schema.GroupVersionKind
}

// GraphBuilder 处理 informer 提供的事件，更新缓存已知依赖关系的 uidToNode 图，
// 并把需要处理的节点放入 attemptToDelete 和 attemptToOrphan 队列。
type GraphBuilder struct {
	restMapper meta.RESTMapper

	// 每个 monitor 负责 list/watch 一种资源，结果统一汇入依赖图构建器。
	monitors    monitors
	monitorLock sync.RWMutex
	monitorWG   sync.WaitGroup
	// informersStarted 在所有控制器初始化并运行后关闭；只有在这之后才能在此启动 monitor。
	informersStarted <-chan struct{}

	// stopCh 用于驱动关闭：从其读取被解除阻塞时 monitor 将停止。该 channel 也受 monitorLock 保护。
	stopCh <-chan struct{}

	// running 在 Run() 被调用时置为 true，在 Run() 收到取消信号后恢复为 false。受 monitorLock 保护。
	running bool

	eventRecorder    record.EventRecorder
	eventBroadcaster record.EventBroadcaster

	metadataClient metadata.Interface
	// monitor 是 graphChanges 队列的生产者，graphBuilder 根据变更更新内存中的图。
	graphChanges workqueue.TypedRateLimitingInterface[*event]
	// uidToNode 无需加锁，因为只有单线程的 processGraphChanges() 读写它。
	uidToNode *concurrentUIDToNode
	// GraphBuilder 是 attemptToDelete 和 attemptToOrphan 的生产者，GC 是消费者。
	attemptToDelete workqueue.TypedRateLimitingInterface[*node]
	attemptToOrphan workqueue.TypedRateLimitingInterface[*node]
	// GraphBuilder 与 GC 共享 absentOwnerCache，已确认不存在的对象会被缓存其中。
	absentOwnerCache *ReferenceCache
	sharedInformers  informerfactory.InformerFactory
	ignoredResources map[schema.GroupResource]struct{}
}

// monitor 用一个局部 stop channel 管理 Controller 的运行。
type monitor struct {
	controller cache.Controller
	store      cache.Store

	// stopCh 用于停止 Controller；若为 nil，表示该 monitor 尚未启动。
	stopCh chan struct{}
}

// Run 应在独立 goroutine 中调用；重复调用是错误的。
func (m *monitor) Run() {
	m.controller.Run(m.stopCh)
}

// monitors 按资源 GVR 映射到对应的 monitor。
type monitors map[schema.GroupVersionResource]*monitor

// // NewDependencyGraphBuilder 创建依赖图构建器，初始化各类工作队列、事件记录器和 uidToNode 映射。
func NewDependencyGraphBuilder(
	ctx context.Context,
	metadataClient metadata.Interface,
	mapper meta.ResettableRESTMapper,
	ignoredResources map[schema.GroupResource]struct{},
	sharedInformers informerfactory.InformerFactory,
	informersStarted <-chan struct{},
) *GraphBuilder {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))

	attemptToDelete := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[*node](),
		workqueue.TypedRateLimitingQueueConfig[*node]{
			Name: "garbage_collector_attempt_to_delete",
		},
	)
	attemptToOrphan := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[*node](),
		workqueue.TypedRateLimitingQueueConfig[*node]{
			Name: "garbage_collector_attempt_to_orphan",
		},
	)
	absentOwnerCache := NewReferenceCache(500)
	graphBuilder := &GraphBuilder{
		eventRecorder:    eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "garbage-collector-controller"}),
		eventBroadcaster: eventBroadcaster,
		metadataClient:   metadataClient,
		informersStarted: informersStarted,
		restMapper:       mapper,
		graphChanges: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[*event](),
			workqueue.TypedRateLimitingQueueConfig[*event]{
				Name: "garbage_collector_graph_changes",
			},
		),
		uidToNode: &concurrentUIDToNode{
			uidToNode: make(map[types.UID]*node),
		},
		attemptToDelete:  attemptToDelete,
		attemptToOrphan:  attemptToOrphan,
		absentOwnerCache: absentOwnerCache,
		sharedInformers:  sharedInformers,
		ignoredResources: ignoredResources,
	}

	return graphBuilder
}

// // controllerFor 为指定资源创建（或复用共享）informer，并注册把 add/update/delete
// // 事件转换为 graphChanges 事件的处理函数。
func (gb *GraphBuilder) controllerFor(logger klog.Logger, resource schema.GroupVersionResource, kind schema.GroupVersionKind) (cache.Controller, cache.Store, error) {
	handlers := cache.ResourceEventHandlerFuncs{
		// 将事件包装后加入 graphChanges 队列，交由图构建器异步处理。
		AddFunc: func(obj interface{}) {
			event := &event{
				eventType: addEvent,
				obj:       obj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// TODO：检查 ownerRefs、finalizers 和 DeletionTimestamp 是否变化；无变化时忽略更新。
			event := &event{
				eventType: updateEvent,
				obj:       newObj,
				oldObj:    oldObj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
		DeleteFunc: func(obj interface{}) {
			// DeltaFIFO 可能把对象包装成 DeletedFinalStateUnknown，这里先解包取回真实对象。
			if deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = deletedFinalStateUnknown.Obj
			}
			event := &event{
				eventType: deleteEvent,
				obj:       obj,
				gvk:       kind,
			}
			gb.graphChanges.Add(event)
		},
	}

	shared, err := gb.sharedInformers.ForResource(resource)
	if err != nil {
		logger.V(4).Info("unable to use a shared informer", "resource", resource, "kind", kind, "err", err)
		return nil, nil, err
	}
	logger.V(4).Info("using a shared informer", "resource", resource, "kind", kind)
	// 事件处理函数来自共享缓存，注册时需克隆以避免相互影响。
	shared.Informer().AddEventHandlerWithResyncPeriod(handlers, ResourceResyncTime)
	return shared.Informer().GetController(), shared.Informer().GetStore(), nil
}

// syncMonitors 按给定资源集合重建 monitor 集合，按需创建或删除 monitor。
// 遇到错误时会记录并继续为其余资源创建 monitor，而非立即退出。
// 该方法可在 Run 前后调用；同步过程不会启动 monitor，需另行调用 startMonitors。
func (gb *GraphBuilder) syncMonitors(logger klog.Logger, resources map[schema.GroupVersionResource]struct{}) error {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	toRemove := gb.monitors
	if toRemove == nil {
		toRemove = monitors{}
	}
	current := monitors{}
	errs := []error{}
	kept := 0
	added := 0
	for resource := range resources {
		if _, ok := gb.ignoredResources[resource.GroupResource()]; ok {
			continue
		}
		if m, ok := toRemove[resource]; ok {
			current[resource] = m
			delete(toRemove, resource)
			kept++
			continue
		}
		kind, err := gb.restMapper.KindFor(resource)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't look up resource %q: %v", resource, err))
			continue
		}
		c, s, err := gb.controllerFor(logger, resource, kind)
		if err != nil {
			errs = append(errs, fmt.Errorf("couldn't start monitor for resource %q: %v", resource, err))
			continue
		}
		current[resource] = &monitor{store: s, controller: c}
		added++
	}
	gb.monitors = current

	for _, monitor := range toRemove {
		if monitor.stopCh != nil {
			close(monitor.stopCh)
		}
	}

	logger.V(4).Info("synced monitors", "added", added, "kept", kept, "removed", len(toRemove))
	// NewAggregate 在 errs 为空时返回 nil，无需额外判空。
	return utilerrors.NewAggregate(errs)
}

// startMonitors 确保当前 monitor 集合处于运行状态；新启动的 monitor 还会触发共享 informer 的启动。
// 在 Run 之前调用时不做任何事（此时尚无 stop channel 支撑 monitor/informer 运行）。
func (gb *GraphBuilder) startMonitors(logger klog.Logger) {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	if !gb.running {
		return
	}

	// 等待所有控制器初始化完成、informer 启动之后再继续，避免 monitor 的工作队列收到预期外的事件。
	<-gb.informersStarted

	monitors := gb.monitors
	started := 0
	for _, monitor := range monitors {
		if monitor.stopCh == nil {
			monitor.stopCh = make(chan struct{})
			gb.sharedInformers.Start(gb.stopCh)
			gb.monitorWG.Go(monitor.Run)
			started++
		}
	}
	logger.V(4).Info("started new monitors", "new", started, "current", len(monitors))
}

// stopMonitors 终止所有运行中的 monitor，应在 startMonitors 之后调用以完成清理。
// 若 running 标志未置为 false，本函数会 panic。
func (gb *GraphBuilder) stopMonitors(logger klog.Logger) {
	var total, stopped int

	func() {
		gb.monitorLock.Lock()
		defer gb.monitorLock.Unlock()

		if gb.running {
			panic("must not call stopMonitors while still marked as running")
		}

		total = len(gb.monitors)
		for _, monitor := range gb.monitors {
			if monitor.stopCh != nil {
				stopped++
				close(monitor.stopCh)
			}
		}

		// 清空 monitor 集合，使图构建器可以安全地重新运行或重新同步。
		gb.monitors = nil
	}()

	gb.monitorWG.Wait()
	logger.Info("stopped monitors", "stopped", stopped, "total", total)
}

// IsResourceSynced 在给定资源存在对应 monitor 且已完成同步时返回 true。
func (gb *GraphBuilder) IsResourceSynced(resource schema.GroupVersionResource) bool {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()
	monitor, ok := gb.monitors[resource]
	return ok && monitor.controller.HasSynced()
}

// IsSynced 在存在至少一个 monitor 且所有 monitor 的 controller 均已同步时返回 true。
// 这意味着若所有 monitor 被重建，IsSynced 可能先返回 true 之后又变回 false。
func (gb *GraphBuilder) IsSynced(logger klog.Logger) bool {
	gb.monitorLock.Lock()
	defer gb.monitorLock.Unlock()

	if len(gb.monitors) == 0 {
		logger.V(4).Info("garbage controller monitor not synced: no monitors")
		return false
	}

	for resource, monitor := range gb.monitors {
		if !monitor.controller.HasSynced() {
			logger.V(4).Info("garbage controller monitor not yet synced", "resource", resource)
			return false
		}
	}
	return true
}

// Run 设置 stop channel 并启动 monitor 运行，直到 stopCh 关闭。
// Run 返回前会停止所有仍在运行的 monitor。
func (gb *GraphBuilder) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("Running", "component", "GraphBuilder")
	defer logger.Info("Stopping", "component", "GraphBuilder")

	// 设置 stop channel，供 monitor 与 informer 感知关闭信号。
	gb.monitorLock.Lock()
	gb.stopCh = ctx.Done()
	gb.running = true
	gb.monitorLock.Unlock()

	// 启动 monitor 并持续处理图变更，直到 stop channel 关闭。
	gb.startMonitors(logger)
	wait.Until(func() { gb.runProcessGraphChanges(logger) }, 1*time.Second, ctx.Done())

	// 先标记为未运行，防止再启动新 monitor。
	// 否则 startMonitors 可能在 stopMonitors 之后又启动新 monitor，导致 goroutine 泄漏或死锁。
	gb.monitorLock.Lock()
	gb.running = false
	gb.monitorLock.Unlock()

	// 停止当前运行中的 monitor。
	gb.stopMonitors(logger)
}

// ignoredResources 是垃圾回收器默认忽略的资源集合（当前为 events）。
var ignoredResources = map[schema.GroupResource]struct{}{
	{Group: "", Resource: "events"}:                {},
	{Group: eventv1.GroupName, Resource: "events"}: {},
}

// DefaultIgnoredResources 返回垃圾回收器默认忽略的资源集合。
// 对外暴露是为了让下游集成方获取默认集合，并在构造控制器时按需添加。
func DefaultIgnoredResources() map[schema.GroupResource]struct{} {
	return ignoredResources
}

// enqueueVirtualDeleteEvent 在确认虚拟节点没有对应的真实存储对象后，
// 构造并加入一个虚拟删除事件以便后续处理。
func (gb *GraphBuilder) enqueueVirtualDeleteEvent(ref objectReference) {
	gv, _ := schema.ParseGroupVersion(ref.APIVersion)
	gb.graphChanges.Add(&event{
		virtual:   true,
		eventType: deleteEvent,
		gvk:       gv.WithKind(ref.Kind),
		obj: &metaonly.MetadataOnlyObject{
			TypeMeta:   metav1.TypeMeta{APIVersion: ref.APIVersion, Kind: ref.Kind},
			ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, UID: ref.UID, Name: ref.Name},
		},
	})
}

// addDependentToOwners 把 n 加入各 owner 的 dependents 列表。
// 若某 owner 尚不在 uidToNode 中，会创建一个“虚拟”节点代表它，并加入 attemptToDelete
// 队列，以便 attemptToDeleteItem() 向 API Server 核实该 owner 是否真实存在。
func (gb *GraphBuilder) addDependentToOwners(logger klog.Logger, n *node, owners []metav1.OwnerReference) {
	// 跟踪是否存在这样的 owner：图中已有且已被 informer 观测到，
	// 但 dependent 的 ownerRef 与其观测到的坐标不一致。
	hasPotentiallyInvalidOwnerReference := false

	for _, owner := range owners {
		ownerNode, ok := gb.uidToNode.Read(owner.UID)
		if !ok {
			// owner 尚未出现在图中时，先创建一个虚拟节点占位。
			ownerNode = &node{
				identity: objectReference{
					OwnerReference: ownerReferenceCoordinates(owner),
					Namespace:      n.identity.Namespace,
				},
				dependents: make(map[*node]struct{}),
				virtual:    true,
			}
			logger.V(5).Info("add virtual item", "identity", ownerNode.identity)
			gb.uidToNode.Write(ownerNode)
		}
		ownerNode.addDependent(n)
		if !ok {
			// 把虚拟节点加入 attemptToDelete：若 API Server 确认 owner 不存在，
			// GC 会生成虚拟删除事件将其从图中移除。
			gb.attemptToDelete.Add(ownerNode)
		} else if !hasPotentiallyInvalidOwnerReference {
			ownerIsNamespaced := len(ownerNode.identity.Namespace) > 0
			if ownerIsNamespaced && ownerNode.identity.Namespace != n.identity.Namespace {
				if ownerNode.isObserved() {
					// owner 已被 informer 观测到，而 dependent 的命名空间与之不符，这必然是错误的。
					// 集群级 owner 可被任意命名空间或集群级对象引用，因此此处仅针对命名空间级 owner。
					logger.V(2).Info("item references an owner but does not match namespaces", "item", n.identity, "owner", ownerNode.identity)
					gb.reportInvalidNamespaceOwnerRef(n, owner.UID)
				}
				hasPotentiallyInvalidOwnerReference = true
			} else if !ownerReferenceMatchesCoordinates(owner, ownerNode.identity.OwnerReference) {
				if ownerNode.isObserved() {
					// owner 已被 informer 观测到，而 n 的 ownerRef 与之不符，可能存在问题。
					logger.V(2).Info("item references an owner with coordinates that do not match the observed identity", "item", n.identity, "owner", ownerNode.identity)
				}
				hasPotentiallyInvalidOwnerReference = true
			} else if !ownerIsNamespaced && ownerNode.identity.Namespace != n.identity.Namespace && !ownerNode.isObserved() {
				// ownerNode 是集群级虚拟节点，且与子节点命名空间不符。
				// 它可能是一个被集群级子节点错误引用的、实际缺失的命名空间级对象（issue #98040）。
				// 把该子节点加入 attemptToDelete 以核实父引用。
				hasPotentiallyInvalidOwnerReference = true
			}
		}
	}

	if hasPotentiallyInvalidOwnerReference {
		// 把可能无效的 dependent 加入 attemptToDelete：
		// GC 会核实 ownerRef 是否为悬空引用，若全部 owner 确认不存在则删除该 dependent。
		gb.attemptToDelete.Add(n)
	}
}

// // reportInvalidNamespaceOwnerRef 为跨命名空间引用 owner 的情况发出 Warning 事件。
func (gb *GraphBuilder) reportInvalidNamespaceOwnerRef(n *node, invalidOwnerUID types.UID) {
	var invalidOwnerRef metav1.OwnerReference
	var found = false
	for _, ownerRef := range n.owners {
		if ownerRef.UID == invalidOwnerUID {
			invalidOwnerRef = ownerRef
			found = true
			break
		}
	}
	if !found {
		return
	}
	ref := &v1.ObjectReference{
		Kind:       n.identity.Kind,
		APIVersion: n.identity.APIVersion,
		Namespace:  n.identity.Namespace,
		Name:       n.identity.Name,
		UID:        n.identity.UID,
	}
	invalidIdentity := objectReference{
		OwnerReference: metav1.OwnerReference{
			Kind:       invalidOwnerRef.Kind,
			APIVersion: invalidOwnerRef.APIVersion,
			Name:       invalidOwnerRef.Name,
			UID:        invalidOwnerRef.UID,
		},
		Namespace: n.identity.Namespace,
	}
	gb.eventRecorder.Eventf(ref, v1.EventTypeWarning, "OwnerRefInvalidNamespace", "ownerRef %s does not exist in namespace %q", invalidIdentity, n.identity.Namespace)
}

// insertNode 把节点写入 uidToNode，并把 n 加入 n.owners 中所有 owner 的 dependents 列表。
func (gb *GraphBuilder) insertNode(logger klog.Logger, n *node) {
	gb.uidToNode.Write(n)
	gb.addDependentToOwners(logger, n, n.owners)
}

// removeDependentFromOwners 把 n 从各 owner 的 dependents 列表中移除。
func (gb *GraphBuilder) removeDependentFromOwners(n *node, owners []metav1.OwnerReference) {
	for _, owner := range owners {
		ownerNode, ok := gb.uidToNode.Read(owner.UID)
		if !ok {
			continue
		}
		ownerNode.deleteDependent(n)
	}
}

// removeNode 从 uidToNode 中删除节点，并把 n 从 n.owners 中所有 owner 的 dependents 列表移除。
func (gb *GraphBuilder) removeNode(n *node) {
	gb.uidToNode.Delete(n.identity.UID)
	gb.removeDependentFromOwners(n, n.owners)
}

// ownerRefPair 保存同一 ownerRef 更新前后的值。
type ownerRefPair struct {
	oldRef metav1.OwnerReference
	newRef metav1.OwnerReference
}

// TODO：当引用数量较小时，分析此函数，确认朴素平方复杂度算法是否更快。
// referencesDiffs 比较新旧 ownerRef 列表，返回新增、删除和变更（同 UID 但内容不同）的引用。
func referencesDiffs(old []metav1.OwnerReference, new []metav1.OwnerReference) (added []metav1.OwnerReference, removed []metav1.OwnerReference, changed []ownerRefPair) {
	oldUIDToRef := make(map[string]metav1.OwnerReference)
	for _, value := range old {
		oldUIDToRef[string(value.UID)] = value
	}
	oldUIDSet := sets.StringKeySet(oldUIDToRef)
	for _, value := range new {
		newUID := string(value.UID)
		if oldUIDSet.Has(newUID) {
			if !reflect.DeepEqual(oldUIDToRef[newUID], value) {
				changed = append(changed, ownerRefPair{oldRef: oldUIDToRef[newUID], newRef: value})
			}
			oldUIDSet.Delete(newUID)
		} else {
			added = append(added, value)
		}
	}
	for oldUID := range oldUIDSet {
		removed = append(removed, oldUIDToRef[oldUID])
	}

	return added, removed, changed
}

// deletionStartsWithFinalizer 判断对象是否在本次更新中“开始”删除并带有指定 finalizer，
// 即新对象满足条件而旧对象不满足（或旧对象为空）。
func deletionStartsWithFinalizer(oldObj interface{}, newAccessor metav1.Object, matchingFinalizer string) bool {
	// 新对象未进入删除流程或不含目标 finalizer，直接返回 false。
	if !beingDeleted(newAccessor) || !hasFinalizer(newAccessor, matchingFinalizer) {
		return false
	}

	// 旧对象为空、未进入删除流程或不含目标 finalizer，说明本次更新才开始，返回 true。
	if oldObj == nil {
		return true
	}
	oldAccessor, err := meta.Accessor(oldObj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access oldObj: %v", err))
		return false
	}
	return !beingDeleted(oldAccessor) || !hasFinalizer(oldAccessor, matchingFinalizer)
}

// // beingDeleted 判断对象是否带有删除时间戳，即已进入删除流程。
func beingDeleted(accessor metav1.Object) bool {
	return accessor.GetDeletionTimestamp() != nil
}

// // hasDeleteDependentsFinalizer 判断对象是否带有 foreground 删除 finalizer。
func hasDeleteDependentsFinalizer(accessor metav1.Object) bool {
	return hasFinalizer(accessor, metav1.FinalizerDeleteDependents)
}

// // hasOrphanFinalizer 判断对象是否带有 orphan 删除 finalizer。
func hasOrphanFinalizer(accessor metav1.Object) bool {
	return hasFinalizer(accessor, metav1.FinalizerOrphanDependents)
}

// // hasFinalizer 判断对象的 finalizer 列表中是否包含指定项。
func hasFinalizer(accessor metav1.Object, matchingFinalizer string) bool {
	finalizers := accessor.GetFinalizers()
	return slices.Contains(finalizers, matchingFinalizer)
}

// 直接传入 newAccessor 是因为调用方已经为 newObj 创建过 accessor。
// startsWaitingForDependentsDeleted 判断对象是否开始等待 dependents 被删除（foreground 删除）。
func startsWaitingForDependentsDeleted(oldObj interface{}, newAccessor metav1.Object) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, metav1.FinalizerDeleteDependents)
}

// 直接传入 newAccessor 是因为调用方已经为 newObj 创建过 accessor。
// startsWaitingForDependentsOrphaned 判断对象是否开始等待 dependents 被孤立（orphan 删除）。
func startsWaitingForDependentsOrphaned(oldObj interface{}, newAccessor metav1.Object) bool {
	return deletionStartsWithFinalizer(oldObj, newAccessor, metav1.FinalizerOrphanDependents)
}

// 当一个阻塞删除的 ownerRef 被移除，或被改为 "BlockOwnerDeletion=false" 时，
// 对应的 owner 不再阻塞 dependent 删除，需把相关节点加入 attemptToDelete 队列重新检查。
func (gb *GraphBuilder) addUnblockedOwnersToDeleteQueue(logger klog.Logger, removed []metav1.OwnerReference, changed []ownerRefPair) {
	for _, ref := range removed {
		if ref.BlockOwnerDeletion != nil && *ref.BlockOwnerDeletion {
			node, found := gb.uidToNode.Read(ref.UID)
			if !found {
				logger.V(5).Info("cannot find uid in uidToNode", "uid", ref.UID)
				continue
			}
			gb.attemptToDelete.Add(node)
		}
	}
	for _, c := range changed {
		wasBlocked := c.oldRef.BlockOwnerDeletion != nil && *c.oldRef.BlockOwnerDeletion
		isUnblocked := c.newRef.BlockOwnerDeletion == nil || (c.newRef.BlockOwnerDeletion != nil && !*c.newRef.BlockOwnerDeletion)
		if wasBlocked && isUnblocked {
			node, found := gb.uidToNode.Read(c.newRef.UID)
			if !found {
				logger.V(5).Info("cannot find uid in uidToNode", "uid", c.newRef.UID)
				continue
			}
			gb.attemptToDelete.Add(node)
		}
	}
}

// // processTransitions 处理对象的状态转换（如开始 orphan/foreground 删除），
// // 把需要处理的节点加入对应队列。
func (gb *GraphBuilder) processTransitions(logger klog.Logger, oldObj interface{}, newAccessor metav1.Object, n *node) {
	if startsWaitingForDependentsOrphaned(oldObj, newAccessor) {
		logger.V(5).Info("add item to attemptToOrphan", "item", n.identity)
		gb.attemptToOrphan.Add(n)
		return
	}
	if startsWaitingForDependentsDeleted(oldObj, newAccessor) {
		logger.V(2).Info("add item to attemptToDelete, because it's waiting for its dependents to be deleted", "item", n.identity)
		// 若 n 是作为虚拟节点加入的，其 deletingDependents 字段未正确设置，这里总是显式设置一次。
		n.markDeletingDependents()
		for dep := range n.dependents {
			gb.attemptToDelete.Add(dep)
		}
		gb.attemptToDelete.Add(n)
	}
}

// // runProcessGraphChanges 持续处理 graphChanges 队列，直到队列关闭。
func (gb *GraphBuilder) runProcessGraphChanges(logger klog.Logger) {
	for gb.processGraphChanges(logger) {
	}
}

// // identityFromEvent 根据事件中的 GVK 和对象元数据构造对象引用标识。
func identityFromEvent(event *event, accessor metav1.Object) objectReference {
	return objectReference{
		OwnerReference: metav1.OwnerReference{
			APIVersion: event.gvk.GroupVersion().String(),
			Kind:       event.gvk.Kind,
			UID:        accessor.GetUID(),
			Name:       accessor.GetName(),
		},
		Namespace: accessor.GetNamespace(),
	}
}

// processGraphChanges 从 graphChanges 取出事件、更新内存图，并填充待处理队列。
func (gb *GraphBuilder) processGraphChanges(logger klog.Logger) bool {
	item, quit := gb.graphChanges.Get()
	if quit {
		return false
	}
	defer gb.graphChanges.Done(item)
	event := item
	obj := item.obj
	accessor, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("cannot access obj: %v", err))
		return true
	}

	logger.V(5).Info("GraphBuilder process object",
		"apiVersion", event.gvk.GroupVersion().String(),
		"kind", event.gvk.Kind,
		"object", klog.KObj(accessor),
		"uid", string(accessor.GetUID()),
		"eventType", event.eventType,
		"virtual", event.virtual,
	)

	// 先检查图中是否已存在该 UID 对应的节点。
	existingNode, found := gb.uidToNode.Read(accessor.GetUID())
	if found && !event.virtual && !existingNode.isObserved() {
		// 将节点标记为已通过 informer 观测到：
		// 1. 这依赖 graphChanges 只包含真实 informer 的 add/update 事件；
		// 2. 使跟踪虚拟节点存在的逻辑可停止轮询，改为依赖 informer 事件。
		observedIdentity := identityFromEvent(event, accessor)
		if observedIdentity != existingNode.identity {
			// 找出 ownerRef 与观测到的身份不符的 dependents。
			_, potentiallyInvalidDependents := partitionDependents(existingNode.getDependents(), observedIdentity)
			// 把这些可能无效的 dependent 加入 attemptToDelete；若其 owner 依然有效则该操作为空操作。
			// 这覆盖了“先观测到错误子节点、后观测到正确父节点”的顺序；
			// 相反的“先父后子”顺序由 addDependentToOwners 处理。
			for _, dep := range potentiallyInvalidDependents {
				if len(observedIdentity.Namespace) > 0 && dep.identity.Namespace != observedIdentity.Namespace {
					// 命名空间不匹配，必然是错误引用。
					logger.V(2).Info("item references an owner but does not match namespaces",
						"item", dep.identity,
						"owner", observedIdentity,
					)
					gb.reportInvalidNamespaceOwnerRef(dep, observedIdentity.UID)
				}
				gb.attemptToDelete.Add(dep)
			}

			// 先克隆节点以免原地修改，再写入观测到的身份并替换虚拟节点。
			logger.V(2).Info("replacing virtual item with observed item",
				"virtual", existingNode.identity,
				"observed", observedIdentity,
			)
			existingNode = existingNode.clone()
			existingNode.identity = observedIdentity
			gb.uidToNode.Write(existingNode)
		}
		existingNode.markObserved()
	}
	switch {
	case (event.eventType == addEvent || event.eventType == updateEvent) && !found:
		newNode := &node{
			identity:           identityFromEvent(event, accessor),
			dependents:         make(map[*node]struct{}),
			owners:             accessor.GetOwnerReferences(),
			deletingDependents: beingDeleted(accessor) && hasDeleteDependentsFinalizer(accessor),
			beingDeleted:       beingDeleted(accessor),
		}
		gb.insertNode(logger, newNode)
		// 底层 DeltaFIFO 可能把创建与删除合并为一个事件，因此还需进一步处理状态转换。
		gb.processTransitions(logger, event.oldObj, accessor, newNode)
	case (event.eventType == addEvent || event.eventType == updateEvent) && found:
		// 处理 ownerReferences 的变化。
		added, removed, changed := referencesDiffs(existingNode.owners, accessor.GetOwnerReferences())
		if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
			// 检查依赖图变化是否解除了某些 owner 对其 dependent 删除的阻塞。
			gb.addUnblockedOwnersToDeleteQueue(logger, removed, changed)
			// 更新节点自身的 owner 列表。
			existingNode.setOwners(accessor.GetOwnerReferences())
			// 把节点加入新 owner 的 dependents 列表。
			gb.addDependentToOwners(logger, existingNode, added)
			// 把节点从已不再是其 owner 的节点 dependents 列表中移除。
			gb.removeDependentFromOwners(existingNode, removed)
		}

		if beingDeleted(accessor) {
			existingNode.markBeingDeleted()
		}
		gb.processTransitions(logger, event.oldObj, accessor, existingNode)
	case event.eventType == deleteEvent:
		if !found {
			logger.V(5).Info("item doesn't exist in the graph, this shouldn't happen",
				"item", accessor.GetUID(),
			)
			return true
		}

		removeExistingNode := true

		if event.virtual {
			// 这是虚拟删除事件，并非来自 informer 的真实观测。
			deletedIdentity := identityFromEvent(event, accessor)
			if existingNode.virtual {

				// 图中现有节点也是虚拟节点，其坐标尚不确定。
				// 检查是否有 dependents 用与本次虚拟删除事件不同的坐标引用该 owner。
				if matchingDependents, nonmatchingDependents := partitionDependents(existingNode.getDependents(), deletedIdentity); len(nonmatchingDependents) > 0 {

					// 部分 dependent 的坐标与本次事件不一致，不能据此删除现有虚拟节点。
					removeExistingNode = false

					if len(matchingDependents) > 0 {
						// 把观测到的已删除身份记入 absentOwnerCache。
						gb.absentOwnerCache.Add(deletedIdentity)
						// 尝试删除那些确实引用了已确认消失身份的 dependents。
						for _, dep := range matchingDependents {
							gb.attemptToDelete.Add(dep)
						}
					}

					// 若该删除事件已证实 existingNode.identity 确实不存在……
					if existingNode.identity == deletedIdentity {
						// 为那些坐标不匹配的 dependents 寻找一个替代的 owner 身份。
						replacementIdentity := getAlternateOwnerIdentity(nonmatchingDependents, deletedIdentity)
						if replacementIdentity != nil {
							// 用其中一个候选身份构造新节点替换现有虚拟节点。
							replacementNode := existingNode.clone()
							replacementNode.identity = *replacementIdentity
							gb.uidToNode.Write(replacementNode)
							// 再把新虚拟节点放回 attemptToDelete 队列继续核实。
							gb.attemptToDelete.AddRateLimited(replacementNode)
						}
					}
				}

			} else if existingNode.identity != deletedIdentity {
				// 不能仅凭虚拟删除事件就移除图中已通过 informer 观测到的真实节点。
				removeExistingNode = false

				// 已观测到的真实节点坐标与虚拟删除事件不符。
				matchingDependents, _ := partitionDependents(existingNode.getDependents(), deletedIdentity)

				if len(matchingDependents) > 0 {
					// 把观测到的已删除身份记入 absentOwnerCache。
					gb.absentOwnerCache.Add(deletedIdentity)
					// 尝试删除确实引用已确认消失身份的 dependents。
					for _, dep := range matchingDependents {
						gb.attemptToDelete.Add(dep)
					}
				}
			}
		}

		if removeExistingNode {
			// removeNode 负责更新图结构。
			gb.removeNode(existingNode)
			existingNode.dependentsLock.RLock()
			defer existingNode.dependentsLock.RUnlock()
			if len(existingNode.dependents) > 0 {
				gb.absentOwnerCache.Add(identityFromEvent(event, accessor))
			}
			for dep := range existingNode.dependents {
				gb.attemptToDelete.Add(dep)
			}
			for _, owner := range existingNode.owners {
				ownerNode, found := gb.uidToNode.Read(owner.UID)
				if !found || !ownerNode.isDeletingDependents() {
					continue
				}
				// 让 attemptToDeleteItem 检查该 owner 的所有 dependents 是否已被删除，若是则删除该 owner。
				gb.attemptToDelete.Add(ownerNode)
			}
		}
	}
	return true
}

// partitionDependents 将 dependents 按 ownerRef 是否与给定身份匹配分成两组。
// 注意：若一个 dependent 对同一 UID 有多个 ownerRef，它可能同时出现在两个列表中。
func partitionDependents(dependents []*node, matchOwnerIdentity objectReference) (matching, nonmatching []*node) {
	ownerIsNamespaced := len(matchOwnerIdentity.Namespace) > 0
	for i := range dependents {
		dep := dependents[i]
		foundMatch := false
		foundMismatch := false
		// 若 dependent 命名空间与 owner 匹配，或 owner 是集群级对象……
		if ownerIsNamespaced && matchOwnerIdentity.Namespace != dep.identity.Namespace {
			// dependent 命名空间与 owner 不符，所有对该父对象的引用都不匹配。
			foundMismatch = true
		} else {
			for _, ownerRef := range dep.owners {
				// ……找到 UID 匹配的 ownerRef……
				if ownerRef.UID == matchOwnerIdentity.UID {
					// ……再检查其所有坐标是否都匹配。
					if ownerReferenceMatchesCoordinates(ownerRef, matchOwnerIdentity.OwnerReference) {
						foundMatch = true
					} else {
						foundMismatch = true
					}
				}
			}
		}

		if foundMatch {
			matching = append(matching, dep)
		}
		if foundMismatch {
			nonmatching = append(nonmatching, dep)
		}
	}
	return matching, nonmatching
}

// // referenceLessThan 为对象引用定义稳定的字典序：Kind > APIVersion > Namespace > Name > UID，
// // 以便在多个候选身份中选出确定的替代者。
func referenceLessThan(a, b objectReference) bool {
	// Kind/APIVersion 比 Namespace 更重要，这样无论对象是集群级还是命名空间级，
	// 不同 Kind 之间都能保持一致的排序。
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.APIVersion != b.APIVersion {
		return a.APIVersion < b.APIVersion
	}
	// Namespace 比 Name 更重要。
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	// Name 比 UID 更重要。
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	// UID 仅为完整性而参与比较；按 UID 查找 owner 的替代身份时，UID 应始终相同。
	if a.UID != b.UID {
		return a.UID < b.UID
	}
	return false
}

// getAlternateOwnerIdentity 在 deps 中查找 UID 与 verifiedAbsentIdentity 相同、
// 但 apiVersion/kind/name 或 namespace 不同的 ownerRef。
// 返回按 referenceLessThan 排序后紧跟在 verifiedAbsentIdentity 之后的第一个候选；
// 若没有排在其后的，则返回排序最靠前的候选；若无任何候选则返回 nil。
func getAlternateOwnerIdentity(deps []*node, verifiedAbsentIdentity objectReference) *objectReference {
	absentIdentityIsClusterScoped := len(verifiedAbsentIdentity.Namespace) == 0

	seenAlternates := map[objectReference]bool{verifiedAbsentIdentity: true}

	// 记录排序最靠前的替代引用。
	var first *objectReference
	// 记录排序紧跟在 verifiedAbsentIdentity 之后的第一个引用。
	var firstFollowing *objectReference

	for _, dep := range deps {
		for _, ownerRef := range dep.owners {
			if ownerRef.UID != verifiedAbsentIdentity.UID {
				// 跳过 UID 不同的引用。
				continue
			}

			if ownerReferenceMatchesCoordinates(ownerRef, verifiedAbsentIdentity.OwnerReference) {
				if absentIdentityIsClusterScoped || verifiedAbsentIdentity.Namespace == dep.identity.Namespace {
					// 跳过与 verifiedAbsentIdentity 完全一致的引用。
					continue
				}
			}

			ref := objectReference{OwnerReference: ownerReferenceCoordinates(ownerRef), Namespace: dep.identity.Namespace}
			if absentIdentityIsClusterScoped && ref.APIVersion == verifiedAbsentIdentity.APIVersion && ref.Kind == verifiedAbsentIdentity.Kind {
				// 由 verifiedAbsentIdentity 可知该 apiVersion/kind 是集群级的，
				// 因此清除替代身份中的命名空间。
				ref.Namespace = ""
			}

			if seenAlternates[ref] {
				// 跳过已处理过的引用。
				continue
			}
			seenAlternates[ref] = true

			if first == nil || referenceLessThan(ref, *first) {
				// 该候选按字典序排在最前。
				first = &ref
			}
			if referenceLessThan(verifiedAbsentIdentity, ref) && (firstFollowing == nil || referenceLessThan(ref, *firstFollowing)) {
				// 该候选是字典序中紧跟在 verifiedAbsentIdentity 之后的第一个。
				firstFollowing = &ref
			}
		}
	}

	// 优先返回字典序紧随已确认消失身份之后的候选。
	if firstFollowing != nil {
		return firstFollowing
	}
	// 否则返回字典序最靠前的候选。
	return first
}

// // GetGraphResources 返回图构建器持有的删除/孤立队列及缺失 owner 缓存，供 GC 使用。
func (gb *GraphBuilder) GetGraphResources() (
	attemptToDelete workqueue.TypedRateLimitingInterface[*node],
	attemptToOrphan workqueue.TypedRateLimitingInterface[*node],
	absentOwnerCache *ReferenceCache,
) {
	return gb.attemptToDelete, gb.attemptToOrphan, gb.absentOwnerCache
}

// Monitor 暴露 monitor 持有的 store 与 controller，供状态查询使用。
type Monitor struct {
	Store      cache.Store
	Controller cache.Controller
}

// GetMonitor 返回给定资源对应的 monitor 及其是否已完成同步。
func (gb *GraphBuilder) GetMonitor(ctx context.Context, resource schema.GroupVersionResource) (*Monitor, bool, error) {
	gb.monitorLock.RLock()
	defer gb.monitorLock.RUnlock()

	monitor, ok := gb.monitors[resource]
	if !ok {
		return nil, false, fmt.Errorf("no monitor found for resource %s", resource.String())
	}

	resourceMonitor := &Monitor{
		Store:      monitor.store,
		Controller: monitor.controller,
	}

	if !cache.WaitForNamedCacheSyncWithContext(
		ctx,
		func() bool {
			return monitor.controller.HasSynced()
		},
	) {
		return nil, false, nil
	}

	return resourceMonitor, true, nil
}

// // Name 返回图构建器的组件名称。
func (gb *GraphBuilder) Name() string {
	return "dependencygraphbuilder"
}
