/*
Copyright 2024 The Kubernetes Authors.

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

package queue

import (
	"container/list"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/backend/heap"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// activeQueuer 封装 activeQ 的相关操作，其方法内部自行加锁。
type activeQueuer interface {
	underLock(func(unlockedActiveQ unlockedActiveQueuer))
	underRLock(func(unlockedActiveQ unlockedActiveQueueReader))

	// delete 从 activeQ 中删除实体，返回被删除的实体，不存在时返回 nil。
	delete(entityLookup framework.QueuedEntityInfo) framework.QueuedEntityInfo
	pop(logger klog.Logger) (framework.QueuedEntityInfo, error)
	list() []*v1.Pod
	len() int
	has(entityLookup framework.QueuedEntityInfo) bool
	// add 向 activeQ 添加新实体，event 记录触发此次入队的事件用于指标统计。
	// 注意：该方法不会唤醒阻塞中的 pop()，调用方需自行调用 broadcast()。
	// strategy 是实体之前的排队策略，用于判断它是从其他队列移入还是新增，以正确维护入队指标。
	add(logger klog.Logger, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy)
	// get 返回 activeQ 中与给定实体匹配的实体。
	get(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, bool)

	listInFlightEvents() []interface{}
	listInFlightPods() []*v1.Pod
	clusterEventsForPod(logger klog.Logger, pInfo *framework.QueuedPodInfo) ([]*clusterEvent, error)
	addEventsIfPodInFlight(oldPod, newPod *v1.Pod, events []fwk.ClusterEvent) bool
	addEventIfAnyInFlight(oldObj, newObj interface{}, event fwk.ClusterEvent) bool

	isLastPoppedEntity(entityLookup framework.QueuedEntityInfo) bool
	clearPoppedEntity()

	schedulingCycle() int64
	done(pod types.UID)
	close()
	broadcast()
}

// unlockedActiveQueuer 定义自身不加锁的 activeQ 方法，
// 调用方需通过 underLock() 保护。
type unlockedActiveQueuer interface {
	unlockedActiveQueueReader
	// update 在 oldEntity 仍位于队列中时更新其中的 Pod 信息，成功则返回新的 podInfo。
	update(newPod *v1.Pod, oldEntity framework.QueuedEntityInfo) *framework.QueuedPodInfo
	// addEventsIfPodInFlight 在 newPod 处于在途状态时把事件记入 inFlightEvents，
	// 返回是否实际写入。
	addEventsIfPodInFlight(oldPod, newPod *v1.Pod, events []fwk.ClusterEvent) bool
}

// unlockedActiveQueueReader 定义自身不加锁的 activeQ 只读方法，
// 调用方需通过 underLock() 或 underRLock() 保护。
type unlockedActiveQueueReader interface {
	// get 返回 activeQ 中与给定实体匹配的实体，不存在时返回 false。
	// 必须在 activeQueue.underLock() 或 underRLock() 中调用。
	get(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, bool)
}

// unlockedActiveQueue 定义自身不加锁的 activeQ 方法，
// 调用方需通过 underLock() 或 underRLock() 保护。
type unlockedActiveQueue struct {
	queue           *heap.Heap[framework.QueuedEntityInfo]
	inFlightPods    map[types.UID]*list.Element
	inFlightEvents  *list.List
	metricsRecorder *metrics.MetricAsyncRecorder
}

// newUnlockedActiveQueue 创建不加锁的 activeQ 包装器，供 underLock/underRLock 内部使用。
func newUnlockedActiveQueue(queue *heap.Heap[framework.QueuedEntityInfo], inFlightPods map[types.UID]*list.Element, inFlightEvents *list.List, metricsRecorder *metrics.MetricAsyncRecorder) *unlockedActiveQueue {
	return &unlockedActiveQueue{
		queue:           queue,
		inFlightPods:    inFlightPods,
		inFlightEvents:  inFlightEvents,
		metricsRecorder: metricsRecorder,
	}
}

// update 在 oldEntity 仍位于队列中时更新其中的 Pod 信息，成功则返回新的 podInfo。
func (uaq *unlockedActiveQueue) update(newPod *v1.Pod, oldEntity framework.QueuedEntityInfo) *framework.QueuedPodInfo {
	if entity, exists := uaq.queue.Get(oldEntity); exists {
		podInfo, err := entity.Update(newPod)
		if err != nil {
			return nil
		}
		uaq.queue.AddOrUpdate(entity)
		return podInfo
	}
	return nil
}

// addEventsIfPodInFlight 在 newPod 处于在途状态时把事件记入 inFlightEvents，
// 返回是否实际写入。
func (uaq *unlockedActiveQueue) addEventsIfPodInFlight(oldPod, newPod *v1.Pod, events []fwk.ClusterEvent) bool {
	_, ok := uaq.inFlightPods[newPod.UID]
	if ok {
		for _, event := range events {
			uaq.metricsRecorder.ObserveInFlightEventsAsync(event.Label(), 1, false)
			uaq.inFlightEvents.PushBack(&clusterEvent{
				event:  event,
				oldObj: oldPod,
				newObj: newPod,
			})
		}
	}
	return ok
}

// get 返回 activeQ 中与给定实体匹配的实体，不存在时返回 false。
// 必须在 activeQueue.underLock() 或 underRLock() 中调用。
func (uaq *unlockedActiveQueue) get(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, bool) {
	return uaq.queue.Get(entityLookup)
}

// backoffQPopper 定义 activeQ 为空时从退避队列取实体所需的方法。
type backoffQPopper interface {
	// popBackoff 从退避队列取出一个实体。
	popBackoff() (framework.QueuedEntityInfo, error)
	// lenBackoff 返回退避队列的长度。
	lenBackoff() int
}

// activeQueue 实现 activeQueuer，所有字段都必须受 lock 保护。
type activeQueue struct {
	// lock 同步 activeQ 相关操作，保护队列、在途 Pod、在途事件、调度周期与关闭标志。
	// 注意：持有本锁后不得再取 SchedulingQueue.lock，否则可能死锁。
	// 同样不得在持有 backoffQueue.lock 或 nominator.nLock 后取本锁。
	// 正确的加锁顺序为：SchedulingQueue.lock > lock > backoffQueue.lock > nominator.nLock。
	lock sync.RWMutex

	// queue 是调度器主动选取待调度 Pod 的堆，堆顶为优先级最高的实体。
	queue *heap.Heap[framework.QueuedEntityInfo]

	// unlockedQueue 是 queue 的包装器，提供自身不加锁的方法，供 underLock/underRLock 内部调用。
	unlockedQueue *unlockedActiveQueue

	// cond 在实体加入 activeQ 时通知等待者；启用 SchedulerPopFromBackoffQ 时，
	// 实体加入退避队列也会通知。cond 与 lock 配合使用。
	cond sync.Cond

	// inFlightPods 记录已弹出但尚未调用 Done 的 Pod，即正在处理中
	// （调度中、permit 中或绑定周期中）的 Pod。
	// map 的值是该 Pod 在 inFlightEvents 链表中的节点，
	// 节点内保存调度开始时的 Pod 快照，便于日志与调试。
	inFlightPods map[types.UID]*list.Element

	// inFlightEvents 按时间顺序保存调度队列收到的事件（clusterEvent）与在途 Pod（*v1.Pod），
	// 持锁时向链表尾追加，因此事件天然按序。
	// Pod 节点在 Pop 时加入，用于标记该 Pod 调度尝试之后发生的事件；
	// 调度完成后移除，并集中处理期间累积的事件。
	// 移除某个 Pod 后，链表头部那些早于所有现存 Pod 启动的事件不再被任何 Pod 需要，可以清理。
	inFlightEvents *list.List

	// schedCycle 是调度周期序号，每次弹出实体时递增。
	schedCycle int64

	// closed 表示队列已关闭，用于让等待中的 Pop() 退出循环。
	closed bool

	metricsRecorder *metrics.MetricAsyncRecorder

	// backoffQPopper 在 activeQ 为空时从退避队列取实体，
	// 仅当 SchedulerPopFromBackoffQ 特性启用时非 nil。
	backoffQPopper backoffQPopper

	// lastPoppedEntityKey 记录最近弹出的实体，
	// 用于判断当前调度周期是否在等待与新入队 Pod 匹配的 PodGroup。
	// 实体重新入队时应清空该值。
	lastPoppedEntityKey string
}

// newActiveQueue 创建 activeQueue 并初始化条件变量与不加锁包装器。
func newActiveQueue(queue *heap.Heap[framework.QueuedEntityInfo], metricRecorder *metrics.MetricAsyncRecorder, backoffQPopper backoffQPopper) *activeQueue {
	aq := &activeQueue{
		queue:           queue,
		inFlightPods:    make(map[types.UID]*list.Element),
		inFlightEvents:  list.New(),
		metricsRecorder: metricRecorder,
		backoffQPopper:  backoffQPopper,
	}
	aq.cond.L = &aq.lock
	aq.unlockedQueue = newUnlockedActiveQueue(queue, aq.inFlightPods, aq.inFlightEvents, metricRecorder)

	return aq
}

// underLock 在写锁保护下执行 fn。
// fn 只能调用 unlockedActiveQueuer 的方法，调用其他 activeQueue 方法会导致死锁。
func (aq *activeQueue) underLock(fn func(unlockedActiveQ unlockedActiveQueuer)) {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	fn(aq.unlockedQueue)
}

// underRLock 在读锁保护下执行 fn。
// fn 只能调用 unlockedActiveQueueReader 的方法，调用其他 activeQueue 方法会导致死锁。
func (aq *activeQueue) underRLock(fn func(unlockedActiveQ unlockedActiveQueueReader)) {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	fn(aq.unlockedQueue)
}

// delete 从 activeQ 中删除实体，返回被删除的实体，不存在时返回 nil。
func (aq *activeQueue) delete(entityLookup framework.QueuedEntityInfo) framework.QueuedEntityInfo {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	return aq.queue.Delete(entityLookup)
}

// unlockedMoveEntityToInFlight 将实体标记为在途状态，需在持锁情况下调用。
func (aq *activeQueue) unlockedMoveEntityToInFlight(logger klog.Logger, entity framework.QueuedEntityInfo) error {
	entity.IncAttempts()
	var podsToDiscard []*framework.QueuedPodInfo
	entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		// Pod 已在 map 中时不能覆盖，否则原链表节点泄漏。
		// https://github.com/kubernetes/kubernetes/pull/127016
		if _, ok := aq.inFlightPods[pInfo.Pod.UID]; ok {
			podsToDiscard = append(podsToDiscard, pInfo)
			return true
		}

		aq.metricsRecorder.ObserveInFlightEventsAsync(metrics.PodPoppedInFlightEvent, 1, false)
		aq.inFlightPods[pInfo.Pod.UID] = aq.inFlightEvents.PushBack(pInfo.Pod)
		return true
	})
	if len(podsToDiscard) > 0 {
		switch specificEntity := entity.(type) {
		case *framework.QueuedPodInfo:
			return fmt.Errorf("the same pod is tracked in multiple places in the scheduler: %s", klog.KObj(podsToDiscard[0]))
		case *framework.QueuedPodGroupInfo:
			for _, pInfo := range podsToDiscard {
				// Pod 必然属于该 PodGroup，忽略返回值。
				_ = specificEntity.RemovePod(pInfo.Pod)
				logger.Error(nil, "Discarding the popped pod group member. It's tracked in multiple places in the scheduler", "pod", klog.KObj(pInfo), "podGroup", klog.KObj(specificEntity))
			}
			if len(specificEntity.QueuedPodInfos) == 0 {
				return fmt.Errorf("all pods from the pod group are tracked in multiple places in the scheduler: %s", klog.KObj(specificEntity))
			}
		default:
			return fmt.Errorf("unexpected entity type: %T", entity)
		}
	}
	aq.schedCycle++

	// 实体出队后撤销不可调度原因对应的指标计数。
	entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		for plugin := range pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins) {
			metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Dec()
		}
		return true
	})
	return nil
}

// pop 取出并返回队列头部实体。队列为空时阻塞等待新实体入队，
// 每次弹出都会递增调度周期。
func (aq *activeQueue) pop(logger klog.Logger) (framework.QueuedEntityInfo, error) {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	return aq.unlockedPop(logger)
}

// unlockedPop 是不加锁的弹出实现，调用方需持有 aq.lock。
func (aq *activeQueue) unlockedPop(logger klog.Logger) (framework.QueuedEntityInfo, error) {
	var entity framework.QueuedEntityInfo
	for aq.queue.Len() == 0 {
		// backoffQPopper 仅在特性启用时非 nil；退避队列非空时可直接从那里取。
		if aq.backoffQPopper != nil && aq.backoffQPopper.lenBackoff() != 0 {
			break
		}
		// 队列为空时 Pop() 阻塞等待；close() 会置位 closed 并广播条件变量，
		// 使此循环继续执行并返回。
		if aq.closed {
			logger.V(2).Info("Scheduling queue is closed")
			return nil, nil
		}
		aq.cond.Wait()
	}
	entity, err := aq.queue.Pop()
	if err != nil {
		if aq.backoffQPopper == nil {
			return nil, err
		}
		// activeQ 为空时转从退避队列取实体。
		entity, err = aq.backoffQPopper.popBackoff()
		if err != nil {
			return nil, err
		}
		recordIncomingEntitiesMetrics("active", entity, framework.PopFromBackoffQ, nil)
	}
	err = aq.unlockedMoveEntityToInFlight(logger, entity)
	if err != nil {
		// 重复实体只记录错误，无需停止调度器：通常不会造成可见的调度问题。
		utilruntime.HandleErrorWithLogger(logger, err, "Discarding the popped entity", "type", entity.Type(), "entity", klog.KObj(entity))
		// 丢弃该实体并继续尝试弹出下一个。
		return aq.unlockedPop(logger)
	}
	aq.lastPoppedEntityKey = queuedEntityKeyFunc(entity)

	return entity, nil
}

// isLastPoppedEntity 判断最近弹出的实体是否为给定实体。
func (aq *activeQueue) isLastPoppedEntity(entityLookup framework.QueuedEntityInfo) bool {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.lastPoppedEntityKey == queuedEntityKeyFunc(entityLookup)
}

// clearPoppedEntity 清空最近弹出的实体记录。
func (aq *activeQueue) clearPoppedEntity() {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	aq.lastPoppedEntityKey = ""
}

// list 返回队列中所有 Pod。
func (aq *activeQueue) list() []*v1.Pod {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	var result []*v1.Pod
	for _, entity := range aq.queue.List() {
		entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
			result = append(result, pInfo.Pod)
			return true
		})
	}
	return result
}

// len 返回队列长度。
func (aq *activeQueue) len() int {
	return aq.queue.Len()
}

// has 判断实体是否在队列中。
func (aq *activeQueue) has(entityLookup framework.QueuedEntityInfo) bool {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.queue.Has(entityLookup)
}

// add 向 activeQ 添加新实体，event 记录触发此次入队的事件用于指标统计。
// 注意：该方法不会唤醒阻塞中的 pop()，调用方需自行调用 broadcast()。
func (aq *activeQueue) add(logger klog.Logger, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy) {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	aq.queue.AddOrUpdate(entity)
	recordIncomingEntitiesMetrics("active", entity, event, strategy)
	logger.V(5).Info("Entity moved to an internal scheduling queue", "type", entity.Type(), "entity", klog.KObj(entity), "event", event, "queue", activeQ)
}

// get 返回 activeQ 中与给定实体匹配的实体。
func (aq *activeQueue) get(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, bool) {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.unlockedQueue.get(entityLookup)
}

// listInFlightEvents 返回所有在途事件。
func (aq *activeQueue) listInFlightEvents() []interface{} {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	var values []interface{}
	for event := aq.inFlightEvents.Front(); event != nil; event = event.Next() {
		values = append(values, event.Value)
	}
	return values
}

// listInFlightPods 返回所有在途 Pod。
func (aq *activeQueue) listInFlightPods() []*v1.Pod {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	var pods []*v1.Pod
	for _, obj := range aq.inFlightPods {
		pods = append(pods, obj.Value.(*v1.Pod))
	}
	return pods
}

// clusterEventsForPod 获取该 Pod 调度尝试期间发生的全部集群事件。
func (aq *activeQueue) clusterEventsForPod(logger klog.Logger, pInfo *framework.QueuedPodInfo) ([]*clusterEvent, error) {
	aq.lock.RLock()
	defer aq.lock.RUnlock()

	logger.V(5).Info("Checking events for in-flight pod", "pod", klog.KObj(pInfo.Pod), "unschedulablePlugins", pInfo.UnschedulablePlugins, "inFlightEventsSize", aq.inFlightEvents.Len(), "inFlightPodsSize", len(aq.inFlightPods))

	// 调度或绑定结束时会调用 AddUnschedulablePodIfNotPresent，
	// 因此这里的实体必然已被 Pop，应该在 inFlightPods 中有记录。
	inFlightPod, ok := aq.inFlightPods[pInfo.Pod.UID]
	if !ok {
		return nil, fmt.Errorf("in flight entity isn't found in the scheduling queue. If you see this error log, it's likely a bug in the scheduler")
	}

	var events []*clusterEvent
	for event := inFlightPod.Next(); event != nil; event = event.Next() {
		e, ok := event.Value.(*clusterEvent)
		if !ok {
			// 不是事件节点就必然是在途 Pod 节点，忽略即可。
			continue
		}
		events = append(events, e)
	}
	return events, nil
}

// addEventsIfPodInFlight 在 newPod 处于在途状态时把事件记入 inFlightEvents，
// 返回是否实际写入。
func (aq *activeQueue) addEventsIfPodInFlight(oldPod, newPod *v1.Pod, events []fwk.ClusterEvent) bool {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	return aq.unlockedQueue.addEventsIfPodInFlight(oldPod, newPod, events)
}

// addEventIfAnyInFlight 在存在任意在途 Pod 时把集群事件记入 inFlightEvents，
// 返回是否实际写入。
func (aq *activeQueue) addEventIfAnyInFlight(oldObj, newObj interface{}, event fwk.ClusterEvent) bool {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	if len(aq.inFlightPods) != 0 {
		aq.metricsRecorder.ObserveInFlightEventsAsync(event.Label(), 1, false)
		aq.inFlightEvents.PushBack(&clusterEvent{
			event:  event,
			oldObj: oldObj,
			newObj: newObj,
		})
		return true
	}
	return false
}

// schedulingCycle 返回当前调度周期序号。
func (aq *activeQueue) schedulingCycle() int64 {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.schedCycle
}

// done 必须在 Pop 返回的 Pod 处理完成后调用，
// 以便队列跟踪哪些 Pod 仍在处理中。
func (aq *activeQueue) done(podUID types.UID) {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	aq.unlockedDone(podUID)
}

// unlockedDone 是内部实现，自身不加锁，调用方必须已持有锁。
func (aq *activeQueue) unlockedDone(podUID types.UID) {
	inFlightEntity, ok := aq.inFlightPods[podUID]
	if !ok {
		// 该实体已被 done()，直接返回。
		return
	}
	delete(aq.inFlightPods, podUID)

	// 从链表中移除该 Pod 节点。
	aq.inFlightEvents.Remove(inFlightEntity)

	aggrMetricsCounter := map[string]int{}
	// 清理仅被该 Pod 引用的事件，防止 inFlightEvents 无限增长。
	// 若该 Pod 位于链表头部，则它与下一个 Pod 之间的事件都不再需要。
	for {
		e := aq.inFlightEvents.Front()
		if e == nil {
			// 链表已空。
			break
		}
		ev, ok := e.Value.(*clusterEvent)
		if !ok {
			// 遇到 Pod 节点，停止清理。
			break
		}
		aq.inFlightEvents.Remove(e)
		aggrMetricsCounter[ev.event.Label()]--
	}

	for evLabel, count := range aggrMetricsCounter {
		aq.metricsRecorder.ObserveInFlightEventsAsync(evLabel, float64(count), false)
	}

	aq.metricsRecorder.ObserveInFlightEventsAsync(metrics.PodPoppedInFlightEvent, -1,
		// 最后一个在途 Pod 离开时强制刷新指标；否则在小集群中
		// 可能长时间没有新 Pod，指标迟迟得不到刷新。
		len(aq.inFlightPods) == 0)
}

// close 关闭 activeQueue。
func (aq *activeQueue) close() {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	// 关闭时对所有在途 Pod 调用 done 以清理指标。
	// 即使异步绑定周期随后再调用 done 也安全，届时它是空操作。
	for pod := range aq.inFlightPods {
		aq.unlockedDone(pod)
	}
	aq.closed = true
}

// broadcast 通知阻塞中的 pop() 有新实体入队。
func (aq *activeQueue) broadcast() {
	aq.cond.Broadcast()
}
