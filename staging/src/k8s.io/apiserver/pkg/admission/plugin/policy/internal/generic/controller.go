/*
Copyright 2022 The Kubernetes Authors.

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

package generic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/cache/synctrack"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// 编译期断言：controller 实现 Controller 接口。
var _ Controller[runtime.Object] = &controller[runtime.Object]{}

// controller 是 Controller 的唯一实现。每个 key（namespace/name）在同一时刻
// 只由一个 worker 处理，reconciler 因此无需自行加锁。
type controller[T runtime.Object] struct {
	informer Informer[T]
	queue    workqueue.TypedRateLimitingInterface[string]

	// 若 reconciliation 期间出现可重试的瞬时错误，返回 error 以便稍后重试该对象。
	reconciler func(namespace, name string, newObj T) error

	options ControllerOptions

	// hasProcessed 跟踪初始列表中的 key 是否都已至少处理过一次，
	// 用于对外暴露 HasSynced。
	hasProcessed *synctrack.AsyncTracker[string]
}

// ControllerOptions 是控制器的可调参数。
type ControllerOptions struct {
	// Name 用于日志与 workqueue 指标命名。
	Name string
	// Workers 是并发 worker 数；为 0 时默认为 2。
	Workers uint
}

// Informer 返回底层 informer。
func (c *controller[T]) Informer() Informer[T] {
	return c.informer
}

// NewController 创建控制器。queue 延迟到 Run 时才构造，
// 这样同一个 controller 可以重复 Run（例如测试中）。
func NewController[T runtime.Object](
	informer Informer[T],
	reconciler func(namepace, name string, newObj T) error,
	options ControllerOptions,
) Controller[T] {
	if options.Workers == 0 {
		options.Workers = 2
	}

	// 用类型名兜底生成 Name，避免日志与指标中出现空名字。
	if len(options.Name) == 0 {
		options.Name = fmt.Sprintf("%T-controller", *new(T))
	}

	c := &controller[T]{
		options:      options,
		informer:     informer,
		reconciler:   reconciler,
		queue:        nil,
		hasProcessed: synctrack.NewAsyncTracker[string](options.Name),
	}
	return c
}

// Run 运行控制器，返回停止运行的原因。
// context 结束后 reconciliation 立即结束；此时仍在队列中的事件会被丢弃。
func (c *controller[T]) Run(ctx context.Context) error {
	klog.Infof("starting %s", c.options.Name)
	defer klog.Infof("stopping %s", c.options.Name)

	c.queue = workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: c.options.Name},
	)

	// 强制关闭 workqueue，丢弃所有排队项。
	// 必须放在 Run 开头的 defer 中：否则任何提前 return 的路径都会漏掉关闭，
	// 导致 worker 永远阻塞。
	defer c.queue.ShutDown()

	// enqueue 把事件对象转成 key 入队；初始列表中的对象需要登记到 hasProcessed，
	// 否则 HasSynced 永远无法完成。
	enqueue := func(obj interface{}, isInInitialList bool) {
		var key string
		var err error
		if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
			utilruntime.HandleError(err)
			return
		}
		if isInInitialList {
			c.hasProcessed.Start(key)
		}

		c.queue.Add(key)
	}

	registration, err := c.informer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: enqueue,
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldMeta, err1 := meta.Accessor(oldObj)
			newMeta, err2 := meta.Accessor(newObj)

			if err1 != nil || err2 != nil {
				if err1 != nil {
					utilruntime.HandleError(err1)
				}

				if err2 != nil {
					utilruntime.HandleError(err2)
				}
				return
			} else if oldMeta.GetResourceVersion() == newMeta.GetResourceVersion() {
				// ResourceVersion 相同说明对象并未真正变化（informer 重放或 resync），
				// 跳过可避免无谓的重算。测试对象常常不带 RV，单独告警便于排查。
				if len(oldMeta.GetResourceVersion()) == 0 {
					klog.Warningf("%v throwing out update with empty RV. this is likely to happen if a test did not supply a resource version on an updated object", c.options.Name)
				}
				return
			}

			enqueue(newObj, false)
		},
		DeleteFunc: func(obj interface{}) {
			// 删除同样入队，reconcile 时会发现对象不存在并以 nil 通知 reconciler
			enqueue(obj, false)
		},
	})

	// informer 已启动又停止时可能报错
	if err != nil {
		return err
	}

	// 确保提前 return（出错）时也把事件处理器从 informer 上摘除
	defer func() {
		// 移除事件处理器并处理错误。错误只会在错误使用事件处理器 API 时出现。
		if err := c.informer.RemoveEventHandler(registration); err != nil {
			utilruntime.HandleError(err)
		}
	}()

	// 等待初始缓存列表同步完成后再开始 reconcile 对象。
	if !cache.WaitFor(ctx, "caches", c.informer.HasSyncedChecker(), registration.HasSyncedChecker()) {
		// TODO: cache.WaitFor 是否应该返回错误？
		// 是 ctx.Err() 还是 context.Cause(ctx)？
		// 无论哪种，都能让下面 "if err == nil"
		// 这类死代码更加明显。

		// 缓存同步期间 ctx 被取消，提前返回
		err := ctx.Err()
		if err == nil {
			// 上下文未取消，说明同步因其他原因失败
			err = errors.New("cache sync failed")
		}
		return err
	}

	// c.informer 与我们的 handler 都已同步，说明初始列表的 AddFunc（即 enqueue）
	// 与 c.hasProcessed.Start 均已调用 => 上游已完成。
	c.hasProcessed.UpstreamHasSynced()

	waitGroup := sync.WaitGroup{}

	for i := uint(0); i < c.options.Workers; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			wait.Until(c.runWorker, time.Second, ctx.Done())
		}()
	}

	klog.Infof("Started %v workers for %v", c.options.Workers, c.options.Name)

	// 等待 context 取消
	<-ctx.Done()

	// 强制关闭 workqueue，丢弃排队项
	c.queue.ShutDown()

	// workqueue 关闭会让 worker 停止；等待所有 worker 清理完毕
	waitGroup.Wait()

	// worker 只会因为调用方取消 context 而停止
	return ctx.Err()
}

// HasSynced 报告初始列表中的对象是否都至少完成过一次 reconciliation。
func (c *controller[T]) HasSynced() bool {
	return c.hasProcessed.HasSynced()
}

// runWorker 循环从队列取 key 并处理，直到队列关闭。
func (c *controller[T]) runWorker() {
	for {
		key, shutdown := c.queue.Get()
		if shutdown {
			return
		}

		// 用匿名函数包住这段逻辑，以便 defer c.workqueue.Done。
		err := func(obj string) error {
			// 调用 Done 告知 workqueue 本项已处理完毕。若不希望该项被重新入队，
			// 还必须调用 Forget；例如遇到瞬时错误时就不调用 Forget，
			// 而是让该项在退避后重试。
			defer c.queue.Done(obj)
			defer c.hasProcessed.Finished(key)

			if err := c.reconcile(key); err != nil {
				// 把该项放回 workqueue，按退避策略重试瞬时错误。
				c.queue.AddRateLimited(key)
				return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
			}
			// 未出错则 Forget，使该项可以立即重新入队而不受长时间限流影响
			c.queue.Forget(obj)
			klog.V(4).Infof("syncAdmissionPolicy(%q)", key)
			return nil
		}(key)

		if err != nil {
			utilruntime.HandleError(err)
		}
	}
}

// reconcile 根据 key 从 informer 取最新对象并调用 reconciler。
// 对象已删除时以 nil 调用 reconciler，让上层清理对应状态。
func (c *controller[T]) reconcile(key string) error {
	var newObj T
	var err error
	var namespace string
	var name string
	var lister NamespacedLister[T]

	// 把 namespace/name 形式的 key 拆成独立的 namespace 与 name
	namespace, name, err = cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// 非法 key 无法重试，丢弃但记录错误
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// 命名空间非空时使用按命名空间过滤的 lister，否则使用全局 lister
	if len(namespace) > 0 {
		lister = c.informer.Namespaced(namespace)
	} else {
		lister = c.informer
	}

	newObj, err = lister.Get(name)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return err
		}

		// 对象已删除，以空对象通知 reconciler
	}

	return c.reconciler(namespace, name, newObj)
}
