/*
Copyright 2025 The Kubernetes Authors.

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

// Package queue 实现调度器的调度队列，包括活跃队列与退避队列。
package queue

import (
	"math"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/backend/heap"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/utils/clock"
)

// backoffQOrderingWindowDuration 是 entityBackoffQ 的排序窗口长度。
// 每个窗口以整秒表示，窗口内按优先级排序；它同时也是把元素从 entityBackoffQ 冲刷到 activeQ 的间隔，
// 以便一次冲刷完整窗口。仅当 PopFromBackoffQ 特性开启时生效，设计动机见 KEP-5142（http://kep.k8s.io/5142）。
const backoffQOrderingWindowDuration = time.Second

// backoffQueuer 封装退避队列（backoffQ）的相关操作。
// 依赖队列本身的方法会在内部加锁。
type backoffQueuer interface {
	// isEntityBackingoff 判断实体是否仍在等待退避计时器；返回 true 时不应重试。
	// 若实体的退避时间落在当前排序窗口内，仍视为处于退避中。
	isEntityBackingoff(entity framework.QueuedEntityInfo) bool
	// popAllBackoffCompleted 从 entityBackoffQ 和 entityErrorBackoffQ 中弹出所有已完成退避的实体。
	popAllBackoffCompleted(logger klog.Logger) []framework.QueuedEntityInfo

	// podInitialBackoffDuration 返回 Pod 可获得的最小退避时长。
	podInitialBackoffDuration() time.Duration
	// podMaxBackoffDuration 返回 Pod 可获得的最大退避时长。
	podMaxBackoffDuration() time.Duration
	// waitUntilAlignedWithOrderingWindow 先等到时间到达排序窗口的整数倍，
	// 之后用 ticker 按窗口间隔执行 f。冲刷时刻必须与窗口对齐：entityBackoffQ 按窗口排序，
	// 整个窗口需一次性冲刷，避免出现可见延迟。
	waitUntilAlignedWithOrderingWindow(f func(), stopCh <-chan struct{})

	// add 把实体加入退避队列。event 记录触发此次入队的原因，用于指标采集；
	// 同时保证实体不会同时存在于两个队列中。strategy 是实体先前的排队策略，
	// 用于判断实体是移到另一队列还是新加入的，以正确维护入队指标。
	add(logger klog.Logger, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy)
	// update 当 oldEntity 已在队列中时更新对应 Pod，返回新的 Pod 信息；未更新则返回 nil。
	update(newPod *v1.Pod, oldEntity framework.QueuedEntityInfo) *framework.QueuedPodInfo
	// delete 从退避队列删除实体；找到则返回被删实体，否则返回 nil。
	delete(entityLookup framework.QueuedEntityInfo) framework.QueuedEntityInfo
	// get 返回与 entityLookup 匹配的实体（如果存在）。
	get(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, bool)
	// has 判断实体是否存在于队列中。
	has(entityLookup framework.QueuedEntityInfo) bool
	// list 返回队列中的所有 Pod。
	list() []*v1.Pod
	// len 返回队列长度。
	len() int
}

// backoffQueue 实现 backoffQueuer，内部封装两个队列，对外表现得像一个队列。
type backoffQueue struct {
	// lock 同步所有 backoffQ 相关操作，同时保护 entityBackoffQ 与 entityErrorBackoffQ。
	// 注意：持有 lock 后不得再获取 SchedulingQueue.lock 或 activeQueue.lock，否则会死锁；
	// 也不得在持有 nominator.nLock 后再获取 lock。
	// 正确的加锁顺序：SchedulingQueue.lock > activeQueue.lock > lock > nominator.nLock。
	lock sync.RWMutex

	clock clock.WithTicker

	// entityBackoffQ 按退避到期时间排序；调度器查看 activeQ 前，会先弹出其中已完成退避的 Pod。
	entityBackoffQ *heap.Heap[framework.QueuedEntityInfo]
	// entityErrorBackoffQ 按错误退避到期时间排序；同样在查看 activeQ 前优先弹出已完成退避的 Pod。
	entityErrorBackoffQ *heap.Heap[framework.QueuedEntityInfo]

	podInitialBackoff time.Duration
	podMaxBackoff     time.Duration
	// 开启 SchedulerPopFromBackoffQ 时，若两个实体退避时间相同，用 activeQLessFn 作为最终比较函数。
	activeQLessFn func(entity1, entity2 framework.QueuedEntityInfo) bool

	// isPopFromBackoffQEnabled 表示特性门控 SchedulerPopFromBackoffQ 是否开启。
	isPopFromBackoffQEnabled bool
}

// newBackoffQueue 创建退避队列：根据特性开关选择排序函数，并为两个内部堆分别挂上指标记录器。
func newBackoffQueue(clock clock.WithTicker, podInitialBackoffDuration time.Duration, podMaxBackoffDuration time.Duration, activeQLessFn func(entity1, entity2 framework.QueuedEntityInfo) bool, popFromBackoffQEnabled bool) *backoffQueue {
	bq := &backoffQueue{
		clock:                    clock,
		podInitialBackoff:        podInitialBackoffDuration,
		podMaxBackoff:            podMaxBackoffDuration,
		isPopFromBackoffQEnabled: popFromBackoffQEnabled,
		activeQLessFn:            activeQLessFn,
	}
	entityBackoffQLessFn := bq.lessBackoffCompleted
	if popFromBackoffQEnabled {
		entityBackoffQLessFn = bq.lessBackoffCompletedWithPriority
	}
	// 错误退避队列始终只按退避时间排序；优先级排序只用于普通退避队列。
	bq.entityBackoffQ = heap.NewWithRecorder(queuedEntityKeyFunc, entityBackoffQLessFn, metrics.NewBackoffEntitiesRecorder())
	bq.entityErrorBackoffQ = heap.NewWithRecorder(queuedEntityKeyFunc, bq.lessBackoffCompleted, metrics.NewBackoffEntitiesRecorder())

	return bq
}

// podInitialBackoffDuration 返回 Pod 可获得的最小退避时长。
func (bq *backoffQueue) podInitialBackoffDuration() time.Duration {
	return bq.podInitialBackoff
}

// podMaxBackoffDuration 返回 Pod 可获得的最大退避时长。
func (bq *backoffQueue) podMaxBackoffDuration() time.Duration {
	return bq.podMaxBackoff
}

// alignToWindow 将时间截断到 entityBackoffQ 的排序窗口起点；特性未开启时原样返回。
func (bq *backoffQueue) alignToWindow(t time.Time) time.Time {
	if !bq.isPopFromBackoffQEnabled {
		return t
	}
	return t.Truncate(backoffQOrderingWindowDuration)
}

// waitUntilAlignedWithOrderingWindow 先等到时间到达排序窗口的整数倍，
// 之后用 ticker 按窗口间隔执行 f。冲刷时刻必须与窗口对齐：排序按窗口进行，
// 整个窗口需一次性冲刷，避免产生可见延迟。
func (bq *backoffQueue) waitUntilAlignedWithOrderingWindow(f func(), stopCh <-chan struct{}) {
	now := bq.clock.Now()
	// 先算出到下一个窗口起点还需等待多久。
	durationToNextWindow := bq.alignToWindow(now.Add(backoffQOrderingWindowDuration)).Sub(now)
	timer := bq.clock.NewTimer(durationToNextWindow)
	select {
	case <-stopCh:
		timer.Stop()
		return
	case <-timer.C():
	}

	// 用 ticker 让后续每次调用 f 都与退避队列的排序窗口保持对齐。
	ticker := bq.clock.NewTicker(backoffQOrderingWindowDuration)
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		f()

		// 注意：Go 的 select 不保证优先级，ticker.C 与 stopCh 同时就绪时可能选中 ticker.C。
		// 因此每轮循环开头都重新检查 stopCh，避免 f() 被多执行一次。
		select {
		case <-stopCh:
			ticker.Stop()
			return
		case <-ticker.C():
		}
	}
}

// lessBackoffCompletedWithPriority 是开启 PopFromBackoffQ 时 entityBackoffQ 的比较函数。
// 同一排序窗口内按 activeQ 的顺序排列，使 activeQ 为空时从退避队列弹出的顺序更合理。
func (bq *backoffQueue) lessBackoffCompletedWithPriority(entity1, entity2 framework.QueuedEntityInfo) bool {
	bo1 := bq.getBackoffTime(entity1)
	bo2 := bq.getBackoffTime(entity2)
	if !bo1.Equal(bo2) {
		return bo1.Before(bo2)
	}
	// 退避时间相同时用 activeQ 的规则比较，保持整体出队顺序一致。
	return bq.activeQLessFn(entity1, entity2)
}

// lessBackoffCompleted 是 entityErrorBackoffQ 的比较函数，仅按退避到期时间排序。
func (bq *backoffQueue) lessBackoffCompleted(entity1, entity2 framework.QueuedEntityInfo) bool {
	bo1 := bq.getBackoffTime(entity1)
	bo2 := bq.getBackoffTime(entity2)
	return bo1.Before(bo2)
}

// isEntityBackingoff 判断实体是否仍在等待退避计时器；返回 true 时不应重试。
// 若退避时间落在当前排序窗口内，仍视为处于退避中。
func (bq *backoffQueue) isEntityBackingoff(entity framework.QueuedEntityInfo) bool {
	boTime := bq.getBackoffTime(entity)
	// 用 !Before 而非 After：退避时间与当前窗口相等时也要返回 true，等下一窗口再冲刷。

	return !boTime.Before(bq.alignToWindow(bq.clock.Now()))
}

// getBackoffTime 返回实体完成退避的时间；零时间表示无需退避。
func (bq *backoffQueue) getBackoffTime(entity framework.QueuedEntityInfo) time.Time {
	if bq.podMaxBackoff == 0 {
		// maxBackoff 为 0 表示完全禁用退避。
		return time.Time{}
	}
	count := entity.GetUnschedulableCount()
	if entity.GetConsecutiveErrorsCount() > 0 {
		// 上一轮调度遇到错误时，改用连续错误次数计算退避时长。
		count = entity.GetConsecutiveErrorsCount()
	}

	if count == 0 {
		// 从未参与过调度尝试的实体无需退避。
		return time.Time{}
	}

	if entity.GetBackoffExpiration().IsZero() {
		duration := bq.calculateBackoffDuration(count, entity.Size())
		entity.SetBackoffExpiration(bq.alignToWindow(entity.GetTimestamp().Add(duration)))
	}

	return entity.GetBackoffExpiration()
}

// calculateBackoffDuration 按尝试次数计算退避时长（指数退避），
// 并对大型实体放宽最大退避时长。
func (bq *backoffQueue) calculateBackoffDuration(count int, entitySize int) time.Duration {
	if count == 0 {
		return 0
	}
	maxBackoff := bq.podMaxBackoff
	if entitySize > 1 {
		// 最大退避时长乘以实体内 Pod 数的平方根：大实体需要更长退避，
		// 但平方根增长可避免退避时间失控。
		maxBackoff = time.Duration(float64(maxBackoff) * math.Sqrt(float64(entitySize)))
	}

	shift := count - 1
	if bq.podInitialBackoff > maxBackoff>>shift {
		return maxBackoff
	}
	return time.Duration(bq.podInitialBackoff << shift)
}

// popAllBackoffCompletedWithQueue 从单个堆中弹出所有已完成退避的实体；
// 堆顶仍在退避时即停止，因为堆按退避时间排序，后续元素只会更晚。
func (bq *backoffQueue) popAllBackoffCompletedWithQueue(logger klog.Logger, queue *heap.Heap[framework.QueuedEntityInfo]) []framework.QueuedEntityInfo {
	var poppedEntities []framework.QueuedEntityInfo
	for {
		entity, ok := queue.Peek()
		if !ok || entity == nil {
			break
		}
		if bq.isEntityBackingoff(entity) {
			break
		}
		_, err := queue.Pop()
		if err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Unable to pop entity from backoff queue despite backoff completion", "type", entity.Type(), "entity", klog.KObj(entity))
			break
		}
		poppedEntities = append(poppedEntities, entity)
	}
	return poppedEntities
}

// popAllBackoffCompleted 从 entityBackoffQ 和 entityErrorBackoffQ 中弹出所有已完成退避的实体。
func (bq *backoffQueue) popAllBackoffCompleted(logger klog.Logger) []framework.QueuedEntityInfo {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	// 两个队列都要处理，不能因一个为空就跳过另一个。
	return append(bq.popAllBackoffCompletedWithQueue(logger, bq.entityBackoffQ), bq.popAllBackoffCompletedWithQueue(logger, bq.entityErrorBackoffQ)...)
}

// add 把实体加入退避队列：event 记录触发入队的原因供指标使用，并保证实体不会同时存在于两个队列中。
func (bq *backoffQueue) add(logger klog.Logger, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy) {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	// 不可调度插件与等待插件均为空，说明是调度出错而非不可调度，放入错误退避队列。
	if entity.GetUnschedulablePlugins().Len() == 0 && entity.GetPendingPlugins().Len() == 0 {
		bq.entityErrorBackoffQ.AddOrUpdate(entity)
		// 同一实体不应同时出现在两个退避队列中；发现残留则记录错误。
		if deletedEntity := bq.entityBackoffQ.Delete(entity); deletedEntity != nil {
			logger.Error(nil, "BackoffQueue add() was called with an entity that was already in the entityBackoffQ", "type", entity.Type(), "entity", klog.KObj(entity))
			return
		}
		recordIncomingEntitiesMetrics("backoff", entity, event, strategy)
		logger.V(5).Info("Entity moved to an internal scheduling queue", "type", entity.Type(), "entity", klog.KObj(entity), "event", event, "queue", backoffQ)
		return
	}
	bq.entityBackoffQ.AddOrUpdate(entity)
	// 对称地，清理可能残留在错误退避队列中的同一实体。
	if deletedEntity := bq.entityErrorBackoffQ.Delete(entity); deletedEntity != nil {
		logger.Error(nil, "BackoffQueue add() was called with an entity that was already in the entityErrorBackoffQ", "type", entity.Type(), "entity", klog.KObj(entity))
		return
	}
	recordIncomingEntitiesMetrics("backoff", entity, event, strategy)
	logger.V(5).Info("Entity moved to an internal scheduling queue", "type", entity.Type(), "entity", klog.KObj(entity), "event", event, "queue", backoffQ)
}

// update 当 oldEntity 已在退避队列中时更新其中的 Pod，返回新的 Pod 信息；未找到或未更新则返回 nil。
func (bq *backoffQueue) update(newPod *v1.Pod, oldEntity framework.QueuedEntityInfo) *framework.QueuedPodInfo {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	// 实体可能在普通退避队列或错误退避队列中，分别查找并更新。
	if entity, exists := bq.entityBackoffQ.Get(oldEntity); exists {
		podInfo, err := entity.Update(newPod)
		if err != nil {
			return nil
		}
		bq.entityBackoffQ.AddOrUpdate(entity)
		return podInfo
	}
	// 普通退避队列中没找到，再查错误退避队列。
	if entity, exists := bq.entityErrorBackoffQ.Get(oldEntity); exists {
		podInfo, err := entity.Update(newPod)
		if err != nil {
			return nil
		}
		bq.entityErrorBackoffQ.AddOrUpdate(entity)
		return podInfo
	}
	return nil
}

// delete 从退避队列删除实体；找到则返回被删实体，否则返回 nil。
func (bq *backoffQueue) delete(entityLookup framework.QueuedEntityInfo) framework.QueuedEntityInfo {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	if entity := bq.entityBackoffQ.Delete(entityLookup); entity != nil {
		return entity
	}
	return bq.entityErrorBackoffQ.Delete(entityLookup)
}

// popBackoff 从 entityBackoffQ 弹出一个实体；队列为空时返回错误。
// 注意：不会从 entityErrorBackoffQ 弹出元素。
func (bq *backoffQueue) popBackoff() (framework.QueuedEntityInfo, error) {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	return bq.entityBackoffQ.Pop()
}

// get 查找与 entityLookup 匹配的实体，先查普通退避队列，再查错误退避队列。
func (bq *backoffQueue) get(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, bool) {
	bq.lock.RLock()
	defer bq.lock.RUnlock()

	entity, exists := bq.entityBackoffQ.Get(entityLookup)
	if exists {
		return entity, true
	}
	return bq.entityErrorBackoffQ.Get(entityLookup)
}

// has 判断实体是否存在于两个退避队列中的任意一个。
func (bq *backoffQueue) has(entityLookup framework.QueuedEntityInfo) bool {
	bq.lock.RLock()
	defer bq.lock.RUnlock()

	return bq.entityBackoffQ.Has(entityLookup) || bq.entityErrorBackoffQ.Has(entityLookup)
}

// list 返回两个退避队列中所有实体的 Pod。
func (bq *backoffQueue) list() []*v1.Pod {
	bq.lock.RLock()
	defer bq.lock.RUnlock()

	var result []*v1.Pod
	for _, entity := range bq.entityBackoffQ.List() {
		entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
			result = append(result, pInfo.Pod)
			return true
		})
	}
	for _, entity := range bq.entityErrorBackoffQ.List() {
		entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
			result = append(result, pInfo.Pod)
			return true
		})
	}
	return result
}

// len 返回两个退避队列中实体数量之和。
func (bq *backoffQueue) len() int {
	bq.lock.RLock()
	defer bq.lock.RUnlock()

	return bq.entityBackoffQ.Len() + bq.entityErrorBackoffQ.Len()
}

// lenBackoff 仅返回 entityBackoffQ 的长度，不含错误退避队列。
func (bq *backoffQueue) lenBackoff() int {
	bq.lock.RLock()
	defer bq.lock.RUnlock()

	return bq.entityBackoffQ.Len()
}
