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

package apidispatcher

import (
	"errors"
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/utils/buffer"
)

// queuedAPICall 包含一个 API 调用及 callQueue 处理它所需的字段。
type queuedAPICall struct {
	fwk.APICall
	// onFinish 以非阻塞方式接收 API 调用执行结果。
	// 应始终使用 sendOnFinish 而非直接操作该 channel。
	onFinish chan<- error
	// callID 是本次入队调用的唯一标识。
	// finalize 时用它区分自身与执行期间可能取代它的新调用。
	callID int
}

// sendOnFinish 以非阻塞方式将错误发送到 onFinish channel。
// 应使用它代替直接操作 channel，避免队列无人接收时阻塞。
func (qc *queuedAPICall) sendOnFinish(err error) {
	if qc.onFinish != nil {
		select {
		case qc.onFinish <- err:
		default:
		}
	}
}

// callQueue 管理 API 调用的状态与生命周期，负责调用的合并、入队和收尾。
//
// 对同一对象（以 UID 标识），callsQueue 和 apiCalls 中至多存在一个 API 调用，
// 以避免竞态并保证同一对象上的操作顺序执行。
//
// 为已跟踪对象添加新调用时，会根据相关性决定合并、覆盖旧调用或直接跳过新调用。
// 若旧调用正在执行，新调用会在旧调用 finalize 后才入队。
//
// callQueue 跟踪所有调用的状态（pending、in-flight），
// 以协调 APIDispatcher 管理的异步执行流程。
type callQueue struct {
	lock   sync.RWMutex
	cond   *sync.Cond
	closed bool

	// apiCallRelevances 将 APICallType 映射到相关性值。
	// 同一对象上，相关性更高的调用会覆盖更低的调用。
	// 相同相关性只应赋值给作用于不同对象类型的调用类型。
	apiCallRelevances fwk.APICallRelevances
	// callIDCounter 单调递增，为每个入队的 queuedAPICall 分配唯一 callID。
	callIDCounter int

	// apiCalls 保存已入队或执行中对象的调用详情，需在锁内访问。
	apiCalls map[types.UID]*queuedAPICall
	// callsQueue 是待执行调用的 FIFO 队列，存放对象 UID，需在锁内访问。
	callsQueue buffer.Ring[types.UID]
	// inFlightEntities 保存执行中调用的对象 UID（其执行 goroutine 已被派发），需在锁内访问。
	inFlightEntities sets.Set[types.UID]
}

// newCallQueue 创建一个新的 callQueue。
func newCallQueue(apiCallRelevances fwk.APICallRelevances) *callQueue {
	q := callQueue{
		apiCallRelevances: apiCallRelevances,
		apiCalls:          make(map[types.UID]*queuedAPICall),
		inFlightEntities:  sets.New[types.UID](),
	}
	q.cond = sync.NewCond(&q.lock)

	return &q
}

// isLessRelevant 判断 newCall 的相关性是否低于 oldCall。
func (cq *callQueue) isLessRelevant(oldCall, newCall fwk.APICallType) bool {
	return cq.apiCallRelevances[newCall] < cq.apiCallRelevances[oldCall]
}

// merge 比较同一对象上的新旧 API 调用，按相关性与类型决定跳过、覆盖或合并。
// 必须在持有 cq.lock 时调用。
func (cq *callQueue) merge(oldAPICall, apiCall *queuedAPICall) error {
	if cq.isLessRelevant(oldAPICall.CallType(), apiCall.CallType()) {
		// 新调用相关性更低，直接跳过
		err := fmt.Errorf("a more relevant call is already enqueued for this object: %w", fwk.ErrCallSkipped)
		apiCall.sendOnFinish(err)
		return err
	}
	if oldAPICall.CallType() != apiCall.CallType() {
		if cq.inFlightEntities.Has(oldAPICall.UID()) {
			// 旧调用正在执行，新调用需等它 finalize 后再入队。
			return nil
		}
		// 类型不同且旧调用未执行：覆盖旧调用，并同步修正 pending 指标。
		metrics.AsyncAPIPendingCalls.WithLabelValues(string(oldAPICall.CallType())).Dec()
		metrics.AsyncAPIPendingCalls.WithLabelValues(string(apiCall.CallType())).Inc()
		oldAPICall.sendOnFinish(fmt.Errorf("a more relevant call was enqueued for this object: %w", fwk.ErrCallOverwritten))
		return nil
	}

	// 传入具体的 APICall 对象，便于新方法做类型断言。
	err := apiCall.Merge(oldAPICall.APICall)
	if err != nil {
		err := fmt.Errorf("failed to merge API calls: %w", err)
		apiCall.sendOnFinish(err)
		return err
	}
	oldAPICall.sendOnFinish(fmt.Errorf("a call of the same type was enqueued for this object: %w", fwk.ErrCallOverwritten))

	// 合并后仍需执行该调用（或旧调用正在执行），继续入队流程。
	return nil
}

// enqueue 处理新增或更新调用的入队逻辑。
//
// 先判断调用是否为 no-op：若是且同一对象没有其他执行中的调用，则返回 ErrCallSkipped。
// 否则保存调用；若之前没有跟踪该对象（oldCallPresent 为 false），
// 则将其 UID 加入处理队列等待 worker 取走。
//
// 该逻辑保证同一对象在队列中始终只有一个活跃调用。必须在持有 cq.lock 时调用。
func (cq *callQueue) enqueue(apiCall *queuedAPICall, oldCallPresent bool) error {
	noOp := apiCall.IsNoOp()
	if noOp && !cq.inFlightEntities.Has(apiCall.UID()) {
		// no-op 且旧调用未执行，可以安全跳过；
		// 若有执行中的调用，仍需保留以便其 finalize 后触发后续处理。
		if oldCallPresent {
			cq.removePending(apiCall.UID())
		}
		apiCall.sendOnFinish(fmt.Errorf("call does not need to be executed because it has no effect: %w", fwk.ErrCallSkipped))
		return fmt.Errorf("call does not need to be executed because it has no effect: %w", fwk.ErrCallSkipped)
	}

	objectUID := apiCall.UID()
	cq.apiCalls[objectUID] = apiCall
	if oldCallPresent {
		// 该对象已有调用（pending 或 in-flight），不重复入队；
		// 当前执行中的调用 finalize 时会重新排队最新调用。
		return nil
	}
	cq.callsQueue.WriteOne(objectUID)
	metrics.AsyncAPIPendingCalls.WithLabelValues(string(apiCall.CallType())).Inc()
	cq.cond.Broadcast()
	return nil
}

// removeFromQueue 从队列中移除 objectUID 并返回重建后的队列。
// Ring 不支持原地删除，故遍历重建。
func removeFromQueue(queue *buffer.Ring[types.UID], objectUID types.UID) *buffer.Ring[types.UID] {
	newQueue := buffer.NewRing[types.UID](buffer.RingOptions{
		InitialSize: queue.Len(),
		NormalSize:  queue.Cap(),
	})
	for {
		uid, ok := queue.ReadOne()
		if !ok {
			break
		}
		if uid != objectUID {
			newQueue.WriteOne(uid)
		}
	}
	return newQueue
}

// removePending 从队列及关联数据中移除 objectUID 对应的待执行调用。
// 仅用于尚未出队（非 in-flight）的调用。必须在持有 cq.lock 时调用。
func (cq *callQueue) removePending(objectUID types.UID) {
	apiCall, ok := cq.apiCalls[objectUID]
	if !ok {
		return
	}
	delete(cq.apiCalls, objectUID)
	if !cq.inFlightEntities.Has(objectUID) {
		cq.callsQueue = *removeFromQueue(&cq.callsQueue, objectUID)
		callType := string(apiCall.CallType())
		metrics.AsyncAPIPendingCalls.WithLabelValues(callType).Dec()
	}
}

// add 将新调用加入队列。
// 同一对象已有调用时，按相关性与类型决定跳过、覆盖或合并。
func (cq *callQueue) add(apiCall *queuedAPICall) error {
	cq.lock.Lock()
	defer cq.lock.Unlock()

	apiCall.callID = cq.callIDCounter
	cq.callIDCounter++

	oldAPICall, ok := cq.apiCalls[apiCall.UID()]
	if ok {
		err := cq.merge(oldAPICall, apiCall)
		if err != nil {
			return err
		}
	}
	return cq.enqueue(apiCall, ok)
}

// pop 从队列取出首个对象 UID 并返回对应的调用详情。
// 注意：调用方必须为取出的 apiCall 调用 finalize()，否则会造成内存泄漏。
func (cq *callQueue) pop() (*queuedAPICall, error) {
	cq.lock.Lock()
	defer cq.lock.Unlock()

	for cq.callsQueue.Len() == 0 {
		if cq.closed {
			return nil, nil
		}
		// 队列为空且未关闭，等待新调用入队或关闭信号
		cq.cond.Wait()
	}

	objectUID, ok := cq.callsQueue.ReadOne()
	if !ok {
		return nil, errors.New("api calls queue is empty")
	}
	apiCall, ok := cq.apiCalls[objectUID]
	if !ok {
		return nil, fmt.Errorf("object %s is not present in a map with API calls details", objectUID)
	}
	cq.inFlightEntities.Insert(objectUID)
	callType := string(apiCall.CallType())
	metrics.AsyncAPIPendingCalls.WithLabelValues(callType).Dec()

	return apiCall, nil
}

// finalize 处理已完成的调用。
// 若执行期间同一对象又有新调用到达，则重新入队处理；
// 否则清除已完成调用的详情。必须在调用执行结束后调用。
func (cq *callQueue) finalize(apiCall *queuedAPICall) {
	cq.lock.Lock()
	defer cq.lock.Unlock()

	objectUID := apiCall.UID()
	newAPICall := cq.apiCalls[objectUID]
	if newAPICall.callID == apiCall.callID {
		// map 中的调用未变，直接清除
		delete(cq.apiCalls, objectUID)
	} else {
		// map 中已是新调用，重新入队处理它
		cq.callsQueue.WriteOne(objectUID)
		callType := string(newAPICall.CallType())
		metrics.AsyncAPIPendingCalls.WithLabelValues(callType).Inc()
		cq.cond.Broadcast()
	}
	cq.inFlightEntities.Delete(objectUID)
}

// syncObject 在给定对象与队列中待执行的同对象调用之间做双向同步，
// 返回同步后的对象。若同步后调用变成 no-op 且未执行，则将其移除。
func (cq *callQueue) syncObject(obj metav1.Object) (metav1.Object, error) {
	cq.lock.Lock()
	defer cq.lock.Unlock()

	objectUID := obj.GetUID()
	apiCall, ok := cq.apiCalls[objectUID]
	if !ok {
		return obj, nil
	}

	syncedObj, err := apiCall.Sync(obj)
	noOp := apiCall.IsNoOp()
	if noOp && !cq.inFlightEntities.Has(apiCall.UID()) {
		// 同步后调用变成 no-op 且未在执行，可以移除
		cq.removePending(apiCall.UID())
		apiCall.sendOnFinish(fmt.Errorf("call does not need to be executed because after sync it has no effect: %w", fwk.ErrCallSkipped))
	}

	return syncedObj, err
}

// close 关闭 callQueue，唤醒所有等待的 pop 调用。
func (cq *callQueue) close() {
	cq.lock.Lock()
	defer cq.lock.Unlock()

	cq.closed = true
	cq.cond.Broadcast()
}
