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

package tainteviction

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// WorkArgs 保存传递给定时工作函数的参数。
type WorkArgs struct {
	// Object 是工作项；只有在添加工作项时指定了 UID，这里才会携带 UID。
	Object NamespacedObject
}

// KeyFromWorkArgs 返回 WorkArgs 的键，即工作项的 NamespacedName，忽略 UID。
// 同一 NamespacedName 不会对应多个不同 UID 的工作项，因此该键可安全去重。
func (w *WorkArgs) KeyFromWorkArgs() string {
	return w.Object.NamespacedName.String()
}

// NewWorkArgs 创建不带 UID 的 WorkArgs。
func NewWorkArgs(name, namespace string) *WorkArgs {
	return &WorkArgs{
		Object: NamespacedObject{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}},
	}
}

// TimedWorker 负责在不早于 FireAt 的时刻执行函数。
type TimedWorker struct {
	WorkItem  *WorkArgs
	CreatedAt time.Time
	FireAt    time.Time
	Timer     clock.Timer
	cancelled atomic.Bool
}

// createWorker 创建一个在 fireAt 之后执行 f 的 TimedWorker。
// 若需要立即执行（无延迟），则直接启动并返回 nil，无需分配定时器。
func createWorker(ctx context.Context, wg *sync.WaitGroup, args *WorkArgs, createdAt time.Time, fireAt time.Time, f func(ctx context.Context, fireAt time.Time, args *WorkArgs) error, clock clock.WithDelayedExecution) *TimedWorker {
	delay := fireAt.Sub(createdAt)
	logger := klog.FromContext(ctx)

	worker := TimedWorker{
		WorkItem:  args,
		CreatedAt: createdAt,
		FireAt:    fireAt,
	}

	// cancelled 标志用于保证 Cancel 返回时，工作要么根本不会执行，
	// 要么其 goroutine 已经注册进 WaitGroup。否则可能出现这样的竞态：
	//   1. 定时器触发，进入 wrapper()。
	//   2. Cancel 调用 Timer.Stop，但 goroutine 已进入 wrapper，尚未调用 wg.Go 注册。
	//   3. wg.Wait 因未注册的 goroutine 而提前返回，随后该 goroutine 才启动。
	// 借助该标志，即使出现上述时序，goroutine 也会立即退出，不会执行实际工作。
	wrapper := func() {
		wg.Go(func() {
			if worker.cancelled.Load() {
				return
			}
			if err := f(ctx, fireAt, args); err != nil {
				logger.Error(err, "TaintEvictionController: timed worker failed")
			}
		})
	}
	// 已到期的工作无需定时器，立即执行。
	// 已到期的工作无需定时器，立即执行。
	if delay <= 0 {
		wrapper()
		return nil
	}
	worker.Timer = clock.AfterFunc(delay, wrapper)
	return &worker
}

// Cancel 取消 TimedWorker 的执行。接收者为 nil 时是空操作，
// 便于对"立即执行、没有定时器"的工作统一调用。
func (w *TimedWorker) Cancel() {
	if w != nil {
		// 先置取消标志再停定时器，保证返回时工作要么已经运行、要么不会启动。
		w.cancelled.Store(true)
		w.Timer.Stop()
	}
}

// TimedWorkerQueue 维护一组尚未执行的 TimedWorker，按工作项的键索引。
type TimedWorkerQueue struct {
	sync.Mutex
	// workers 以 KeyFromWorkArgs 的返回值作为键；值为 nil 表示工作无需定时器、已在执行中。
	workers  map[string]*TimedWorker
	workerWG sync.WaitGroup
	workFunc func(ctx context.Context, fireAt time.Time, args *WorkArgs) error
	clock    clock.WithDelayedExecution
}

// CreateWorkerQueue 创建一个 TimedWorkerQueue，其所有工作到期后执行 f。
func CreateWorkerQueue(f func(ctx context.Context, fireAt time.Time, args *WorkArgs) error) *TimedWorkerQueue {
	return &TimedWorkerQueue{
		workers:  make(map[string]*TimedWorker),
		workFunc: f,
		clock:    clock.RealClock{},
	}
}

// getWrappedWorkerFunc 包装实际工作函数，在工作结束后从队列中删除对应条目。
// 工作期间持有锁会阻塞队列操作，因此先执行工作、再加锁清理。
// getWrappedWorkerFunc 包装实际工作函数，在工作结束后从队列中删除对应条目。
// 工作期间持有锁会阻塞队列操作，因此先执行工作、再加锁清理。
func (q *TimedWorkerQueue) getWrappedWorkerFunc(key string) func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
	return func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
		logger := klog.FromContext(ctx)
		logger.V(4).Info("Firing worker", "item", key, "firedTime", fireAt)
		err := q.workFunc(ctx, fireAt, args)
		q.Lock()
		defer q.Unlock()
		logger.V(4).Info("Worker finished, removing", "item", key, "err", err)
		delete(q.workers, key)
		return err
	}
}

// AddWork 向队列添加不早于 fireAt 执行的工作。
// 同名工作已存在时直接跳过：调用方应先使用 UpdateWork 决定是否替换。
func (q *TimedWorkerQueue) AddWork(ctx context.Context, args *WorkArgs, createdAt time.Time, fireAt time.Time) {
	key := args.KeyFromWorkArgs()
	logger := klog.FromContext(ctx)

	q.Lock()
	defer q.Unlock()
	// 同名工作已在队列中，避免重复注册导致同一对象被并发处理。
	// 同名工作已在队列中，避免重复注册导致同一对象被并发处理。
	if _, exists := q.workers[key]; exists {
		logger.V(4).Info("Trying to add already existing work, skipping", "item", key, "createTime", createdAt, "firedTime", fireAt)
		return
	}
	logger.V(4).Info("Adding TimedWorkerQueue item and to be fired at firedTime", "item", key, "createTime", createdAt, "firedTime", fireAt)
	worker := createWorker(ctx, &q.workerWG, args, createdAt, fireAt, q.getWrappedWorkerFunc(key), q.clock)
	q.workers[key] = worker
}

// UpdateWork 添加或替换工作，使其不早于 fireAt 执行。
// 执行时间未变时是廉价空操作；工作已在执行中时也不打断，
// 因为正在执行的工作已读取所需状态，取消它反而会丢失处理。
func (q *TimedWorkerQueue) UpdateWork(ctx context.Context, args *WorkArgs, createdAt time.Time, fireAt time.Time) {
	key := args.KeyFromWorkArgs()
	logger := klog.FromContext(ctx)

	q.Lock()
	defer q.Unlock()
	if worker, exists := q.workers[key]; exists {
		// nil 表示工作已经在执行，保留现状。
		// nil 表示工作已经在执行，保留现状。
		if worker == nil {
			logger.V(4).Info("Keeping existing work, already in progress", "item", key)
			return
		}
		// 触发时间相同，无需 Cancel 再重建定时器。
		// 触发时间相同，无需 Cancel 再重建定时器。
		if worker.FireAt.Compare(fireAt) == 0 {
			logger.V(4).Info("Keeping existing work, same time", "item", key, "createTime", worker.CreatedAt, "firedTime", worker.FireAt)
			return
		}
		logger.V(4).Info("Replacing existing work", "item", key, "createTime", worker.CreatedAt, "firedTime", worker.FireAt)
		worker.Cancel()
	}
	logger.V(4).Info("Adding TimedWorkerQueue item and to be fired at firedTime", "item", key, "createTime", createdAt, "firedTime", fireAt)
	worker := createWorker(ctx, &q.workerWG, args, createdAt, fireAt, q.getWrappedWorkerFunc(key), q.clock)
	q.workers[key] = worker
}

// CancelWork 从队列中移除已排定的工作，返回值表示是否取消了尚未执行的工作。
// key 必须是 WorkArgs.KeyFromWorkArgs（即 NamespacedName.String）的结果。
func (q *TimedWorkerQueue) CancelWork(logger klog.Logger, key string) bool {
	q.Lock()
	defer q.Unlock()
	worker, found := q.workers[key]
	result := false
	if found {
		logger.V(4).Info("Cancelling TimedWorkerQueue item", "item", key, "time", time.Now())
		if worker != nil {
			result = true
			worker.Cancel()
		}
		delete(q.workers, key)
	}
	return result
}

// GetWorkerUnsafe 返回给定键对应的 TimedWorker。
// 不安全之处在于返回的 worker 可能带有定时器，调用后仍会触发，因此仅供测试使用。
func (q *TimedWorkerQueue) GetWorkerUnsafe(key string) *TimedWorker {
	q.Lock()
	defer q.Unlock()
	return q.workers[key]
}

// CancelAndWait 取消所有工作，并等待正在执行的 goroutine 全部退出，
// 用于控制器关闭时确保没有后台工作继续访问已释放的资源。
func (q *TimedWorkerQueue) CancelAndWait() {
	// 必须在释放锁之后 Wait：正在执行的工作完成时需要获取同一把锁清理队列条目，
	// 若持锁等待会形成死锁。
	defer q.workerWG.Wait()
	q.Lock()
	defer q.Unlock()
	for _, worker := range q.workers {
		worker.Cancel()
	}
	q.workers = make(map[string]*TimedWorker)
}
