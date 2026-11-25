/*
Copyright 2017 The Kubernetes Authors.

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
	"sync"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/kubernetes/pkg/controller"
)

// eventType 描述被监控资源发生的变化类型，用于区分配额重新计算的原因。
type eventType int

// String 返回事件类型的可读名称供日志使用；未知类型以编号形式呈现，便于排查异常值。
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

const (
	// addEvent 表示资源被创建；当前未直接使用，保留以完整描述事件种类。
	addEvent eventType = iota
	// updateEvent 表示资源被更新，是否入队由 UpdateFilter 决定。
	updateEvent
	// deleteEvent 表示资源被删除，删除后配额可能需要重新计算。
	deleteEvent
)

// event 是资源变更队列中的元素，携带触发配额重新计算所需的全部信息。
type event struct {
	// eventType 标识变更类型。
	eventType eventType
	// obj 是变更后的对象；删除事件中即为被删除的对象。
	obj interface{}
	// oldObj 仅在更新事件中保存变更前的对象。
	oldObj interface{}
	// gvr 标识对象所属资源，用于定位命名空间与评估器。
	gvr schema.GroupVersionResource
}

// QuotaMonitor 保存跟踪配额与触发配额补充（replenishment）所需的全部状态。
// 它为每个受监控资源维护一个 monitor，并把所有变更汇入同一个工作队列。
type QuotaMonitor struct {
	// monitors 中每个 monitor 负责 list/watch 一种资源，并判断是否需要补充配额。
	monitors    monitors
	monitorWG   sync.WaitGroup
	monitorLock sync.RWMutex
	// informersStarted 在所有控制器初始化并运行后关闭；只有在此之后
	// 才能在这里启动监控器，否则事件可能在队列尚未就绪时到达。
	informersStarted <-chan struct{}

	// stopCh 驱动关闭：一旦可从中接收，监控器就会停止。
	// 该 channel 同样受 monitorLock 保护。
	stopCh <-chan struct{}

	// running 在调用 Run() 后置为 true，在 Run() 收到取消信号后恢复为 false。
	// 它受 monitorLock 保护，用于阻止停止过程中再启动新的监控器。
	running bool

	// resourceChanges 的生产者是各 monitor，事件在此等待被处理。
	resourceChanges workqueue.TypedRateLimitingInterface[*event]

	// informerFactory 提供与 informer 交互的接口。
	informerFactory informerfactory.InformerFactory

	// ignoredResources 列出需要忽略的资源，避免为它们建立监控器。
	ignoredResources map[schema.GroupResource]struct{}

	// resyncPeriod 决定被监控资源的重新同步周期。
	resyncPeriod controller.ResyncPeriodFunc

	// replenishmentFunc 是变更可能触发配额重新计算时的回调。
	replenishmentFunc ReplenishmentFunc

	// registry 维护已注册的配额评估器列表。
	registry quota.Registry

	// updateFilter 决定一次更新是否需要入队；为 nil 时不处理更新事件。
	updateFilter UpdateFilter
}

// NewMonitor 创建 QuotaMonitor 实例，并初始化事件队列与依赖项。
func NewMonitor(informersStarted <-chan struct{}, informerFactory informerfactory.InformerFactory, ignoredResources map[schema.GroupResource]struct{}, resyncPeriod controller.ResyncPeriodFunc, replenishmentFunc ReplenishmentFunc, registry quota.Registry, updateFilter UpdateFilter) *QuotaMonitor {
	return &QuotaMonitor{
		informersStarted: informersStarted,
		informerFactory:  informerFactory,
		ignoredResources: ignoredResources,
		resourceChanges: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[*event](),
			workqueue.TypedRateLimitingQueueConfig[*event]{Name: "resource_quota_controller_resource_changes"},
		),
		resyncPeriod:      resyncPeriod,
		replenishmentFunc: replenishmentFunc,
		registry:          registry,
		updateFilter:      updateFilter,
	}
}

// monitor 用独立的停止 channel 驱动一个 cache.Controller。
type monitor struct {
	controller cache.Controller

	// stopCh 用于停止 Controller；为 nil 表示该 monitor 尚未启动。
	stopCh chan struct{}
}

// Run 应在独立 goroutine 中调用；重复调用会出错，因为同一个 Controller
// 不能被并发运行两次。
func (m *monitor) Run() {
	m.controller.Run(m.stopCh)
}

// monitors 按资源 GVR 索引当前的 monitor 集合。
type monitors map[schema.GroupVersionResource]*monitor

// UpdateFilter 判断一次更新事件是否应加入 resourceChanges 队列。
// 只有真正影响配额用量的更新才值得触发重新计算。
type UpdateFilter func(resource schema.GroupVersionResource, oldObj, newObj interface{}) bool

// controllerFor 为指定资源获取 controller 并注册更新/删除事件处理器。
// 优先复用共享 informer；无法复用时返回错误，交由调用方决定如何处理。
func (qm *QuotaMonitor) controllerFor(ctx context.Context, resource schema.GroupVersionResource) (cache.Controller, error) {
	logger := klog.FromContext(ctx)

	handlers := cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			// 仅当过滤器认定更新影响配额时才入队：多数字段变化与配额无关，
			// 全量入队会带来大量无效重算与 API 调用。
			if qm.updateFilter != nil && qm.updateFilter(resource, oldObj, newObj) {
				event := &event{
					eventType: updateEvent,
					obj:       newObj,
					oldObj:    oldObj,
					gvr:       resource,
				}
				qm.resourceChanges.Add(event)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// delta fifo 在对象删除且未观察到最终状态时可能包一层 DeletedFinalStateUnknown，
			// 必须先解包才能从对象元数据中取到命名空间。
			if deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = deletedFinalStateUnknown.Obj
			}
			event := &event{
				eventType: deleteEvent,
				obj:       obj,
				gvr:       resource,
			}
			qm.resourceChanges.Add(event)
		},
	}
	shared, err := qm.informerFactory.ForResource(resource)
	if err == nil {
		logger.V(4).Info("QuotaMonitor using a shared informer", "resource", resource.String())
		shared.Informer().AddEventHandlerWithResyncPeriod(handlers, qm.resyncPeriod())
		return shared.Informer().GetController(), nil
	}
	logger.V(4).Info("QuotaMonitor unable to use a shared informer", "resource", resource.String(), "err", err)

	// 此处不自建 list/watch：聚合 API 服务器应自行运行配额控制器，
	// 避免本控制器为每个未知资源承担额外存储开销。
	// TODO: 若能与垃圾回收器共享存储，或许可以支持其他资源；
	// 在此之前，聚合 API 服务器必须运行自己的控制器来核对其配额。
	return nil, fmt.Errorf("unable to monitor quota for resource %q", resource.String())
}

// SyncMonitors 根据传入的资源集合重建 monitor 集合，按需创建或删除监控器。
// 即使遇到错误也会尝试为其余资源创建监控器，最后统一返回聚合错误；
// 可在 Run 之前或之后调用。同步本身不会启动 monitor，
// 若要确保所有 monitor 都已启动，需要再调用 StartMonitors。
func (qm *QuotaMonitor) SyncMonitors(ctx context.Context, resources map[schema.GroupVersionResource]struct{}) error {
	logger := klog.FromContext(ctx)

	qm.monitorLock.Lock()
	defer qm.monitorLock.Unlock()

	toRemove := qm.monitors
	if toRemove == nil {
		toRemove = monitors{}
	}
	current := monitors{}
	var errs []error
	kept := 0
	added := 0
	for resource := range resources {
		// 被忽略的资源直接跳过，避免为它们建立无用的 watch。
		if _, ok := qm.ignoredResources[resource.GroupResource()]; ok {
			continue
		}
		if m, ok := toRemove[resource]; ok {
			current[resource] = m
			delete(toRemove, resource)
			kept++
			continue
		}
		c, err := qm.controllerFor(ctx, resource)
		if err != nil {
			// 只记录错误并继续处理其他资源，避免一个资源的失败阻塞整个同步。
			errs = append(errs, fmt.Errorf("couldn't start monitor for resource %q: %v", resource, err))
			continue
		}

		// 若该资源还没有评估器，则注册一个基于对象计数的评估器，
		// 否则配额需求无法被统计。
		evaluator := qm.registry.Get(resource.GroupResource())
		if evaluator == nil {
			listerFunc := generic.ListerFuncForResourceFunc(qm.informerFactory.ForResource)
			listResourceFunc := generic.ListResourceUsingListerFunc(listerFunc, resource)
			evaluator = generic.NewObjectCountEvaluator(resource.GroupResource(), listResourceFunc, "")
			qm.registry.Add(evaluator)
			logger.Info("QuotaMonitor created object count evaluator", "resource", resource.GroupResource())
		}

		// 将新建的 monitor 纳入当前集合；此时尚未启动。
		current[resource] = &monitor{controller: c}
		added++
	}
	qm.monitors = current

	for _, monitor := range toRemove {
		// 只有已启动的 monitor 才有 stopCh，关闭它即可让对应 controller 退出。
		if monitor.stopCh != nil {
			close(monitor.stopCh)
		}
	}

	logger.V(4).Info("quota synced monitors", "added", added, "kept", kept, "removed", len(toRemove))
	// NewAggregate 在 errs 为空时返回 nil，无错误时调用方无需特殊处理。
	return utilerrors.NewAggregate(errs)
}

// StartMonitors 确保当前 monitor 集合均处于运行状态，
// 新启动的 monitor 也会触发共享 informer 的启动。
//
// 若在 Run 之前调用，StartMonitors 不做任何事：此时还没有可用的停止 channel，
// 无法支撑 monitor 与 informer 的生命周期。
func (qm *QuotaMonitor) StartMonitors(ctx context.Context) {
	qm.monitorLock.Lock()
	defer qm.monitorLock.Unlock()

	// 尚未运行（或已经停止）时直接返回，避免在关闭过程中重新启动 monitor。
	if !qm.running {
		return
	}

	// 等待所有控制器初始化后的 informer 启动信号，否则事件可能提前进入
	// 尚未准备好的工作队列，产生意外行为。
	<-qm.informersStarted

	monitors := qm.monitors
	started := 0
	for _, monitor := range monitors {
		// stopCh 为 nil 表示尚未启动；用此判断保证重复调用幂等。
		if monitor.stopCh == nil {
			monitor.stopCh = make(chan struct{})
			qm.informerFactory.Start(qm.stopCh)
			qm.monitorWG.Go(monitor.Run)
			started++
		}
	}
	klog.FromContext(ctx).V(4).Info("QuotaMonitor finished starting monitors", "new", started, "total", len(monitors))
}

// IsSynced 在存在至少一个 monitor 且所有 controller 均已同步时返回 true。
// 注意：若所有 monitor 被重建，IsSynced 可能先返回 true，随后又返回 false。
func (qm *QuotaMonitor) IsSynced(ctx context.Context) bool {
	logger := klog.FromContext(ctx)

	qm.monitorLock.RLock()
	defer qm.monitorLock.RUnlock()

	// 没有 monitor 时不能算已同步，否则调用方会误以为配额数据已经就绪。
	if len(qm.monitors) == 0 {
		logger.V(4).Info("quota monitor not synced: no monitors")
		return false
	}

	for resource, monitor := range qm.monitors {
		if !monitor.controller.HasSynced() {
			logger.V(4).Info("quota monitor not synced", "resource", resource)
			return false
		}
	}
	return true
}

// Run 设置停止 channel 并启动 monitor，直至 ctx 被取消。
// Run 返回前会停止所有正在运行的 monitor。
func (qm *QuotaMonitor) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("QuotaMonitor running")

	// 在锁内发布停止 channel 与运行状态，保证 StartMonitors 看到一致状态。
	qm.monitorLock.Lock()
	qm.stopCh = ctx.Done()
	qm.running = true
	qm.monitorLock.Unlock()

	// 启动 monitor，并开始处理变更事件，直到停止 channel 关闭。
	qm.StartMonitors(ctx)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("QuotaMonitor stopping")
		qm.resourceChanges.ShutDown()
		wg.Wait()
	}()

	wg.Go(func() {
		wait.UntilWithContext(ctx, qm.runProcessResourceChanges, 1*time.Second)
	})

	// 保持阻塞直到被取消；事件处理由上面的 goroutine 负责。
	<-ctx.Done()

	// 停止所有已启动的 monitor。
	qm.monitorLock.Lock()
	defer qm.monitorLock.Unlock()
	// 先标记为未运行，防止停止过程中有新的 monitor 被启动。
	// 否则 StartMonitors 可能在停止之后继续执行，导致 goroutine 泄漏与死锁。
	qm.running = false
	monitors := qm.monitors
	stopped := 0
	for _, monitor := range monitors {
		if monitor.stopCh != nil {
			stopped++
			close(monitor.stopCh)
		}
	}
	qm.monitors = nil
	qm.monitorWG.Wait()
	logger.Info("QuotaMonitor stopped monitors", "stopped", stopped, "total", len(monitors))
}

// runProcessResourceChanges 持续从队列取事件，直到处理函数返回 false（队列关闭）。
func (qm *QuotaMonitor) runProcessResourceChanges(ctx context.Context) {
	for qm.processResourceChanges(ctx) {
	}
}

// processResourceChanges 从 resourceChanges 取出一个事件并触发配额补充回调。
// 返回 false 表示队列已关闭、应停止处理；其余情况均返回 true。
func (qm *QuotaMonitor) processResourceChanges(ctx context.Context) bool {
	item, quit := qm.resourceChanges.Get()
	if quit {
		// 队列已关闭（关闭时阻塞中的 Get 会返回），不再继续处理。
		return false
	}
	defer qm.resourceChanges.Done(item)
	event := item
	obj := event.obj
	accessor, err := meta.Accessor(obj)
	if err != nil {
		// 单个对象无法取元数据时只记录错误并继续：丢弃该事件即可，
		// 返回 true 让队列继续处理后续事件，避免阻塞整个监控流程。
		utilruntime.HandleErrorWithContext(ctx, err, "Cannot access object")
		return true
	}
	klog.FromContext(ctx).V(4).Info("QuotaMonitor process object",
		"resource", event.gvr.String(),
		"namespace", accessor.GetNamespace(),
		"name", accessor.GetName(),
		"uid", string(accessor.GetUID()),
		"eventType", event.eventType,
	)
	qm.replenishmentFunc(ctx, event.gvr.GroupResource(), accessor.GetNamespace())
	return true
}
