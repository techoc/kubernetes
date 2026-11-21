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

package namespace

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/namespace/deletion"

	"k8s.io/klog/v2"
)

const (
	// namespaceDeletionGracePeriod 是处理收到的命名空间事件之前需要等待的时间。
	// 留出这段延迟是为了让以下情况得以发生：
	// * HA apiserver 上的生命周期准入插件也能观察到命名空间删除，
	//   从而阻止在处于 terminating 状态的命名空间中创建新对象
	// * 非 leader 的 etcd 服务器能观察到命名空间中最后一刻的对象创建，
	//   这样本控制器的清理才能真正删除所有对象
	namespaceDeletionGracePeriod = 5 * time.Second
)

// NamespaceController 负责执行依赖于命名空间阶段（phase）的操作，
// 核心职责是在命名空间被请求删除后清理其中的所有资源并移除 finalizer。
type NamespaceController struct {
	// lister 从共享缓存中查询命名空间，避免直接访问 apiserver。
	lister corelisters.NamespaceLister
	// 在命名空间缓存完成首次同步后返回 true。
	listerSynced cache.InformerSynced
	// 等待 worker 处理的命名空间队列（键为命名空间名）。
	queue workqueue.TypedRateLimitingInterface[string]
	// 命名空间被删除时，负责清理其中全部资源的辅助对象。
	namespacedResourcesDeleter deletion.NamespacedResourcesDeleterInterface
}

// NewNamespaceController 创建一个新的 NamespaceController，
// 并注册命名空间 informer 的事件处理器，只把进入删除流程的命名空间入队。
func NewNamespaceController(
	ctx context.Context,
	kubeClient clientset.Interface,
	metadataClient metadata.Interface,
	discoverResourcesFn func() ([]*metav1.APIResourceList, error),
	namespaceInformer coreinformers.NamespaceInformer,
	resyncPeriod time.Duration,
	finalizerToken v1.FinalizerName) *NamespaceController {

	// 先创建控制器对象，便于把它的 enqueueNamespace 注入到事件回调中。
	namespaceController := &NamespaceController{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			nsControllerRateLimiter(),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "namespace",
			},
		),
		namespacedResourcesDeleter: deletion.NewNamespacedResourcesDeleter(ctx, kubeClient.CoreV1().Namespaces(), metadataClient, kubeClient.CoreV1(), discoverResourcesFn, finalizerToken),
	}

	// 配置命名空间 informer 的事件处理器：新增和更新事件都入队，
	// 由 enqueueNamespace 过滤掉尚未进入删除流程的命名空间。
	namespaceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				namespace := obj.(*v1.Namespace)
				namespaceController.enqueueNamespace(ctx, namespace)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				namespace := newObj.(*v1.Namespace)
				namespaceController.enqueueNamespace(ctx, namespace)
			},
		},
		resyncPeriod,
	)
	namespaceController.lister = namespaceInformer.Lister()
	namespaceController.listerSynced = namespaceInformer.Informer().HasSynced

	return namespaceController
}

// nsControllerRateLimiter 返回一个重试速率限制器，其回收周期比常规控制器更快，
// 但保留默认的退避速度和整体重新入队速度。这样设置是因为待删除的命名空间数量
// 通常远少于集群中命名空间级资源的总量，因此可以更快地推进清理。
func nsControllerRateLimiter() workqueue.TypedRateLimiter[string] {
	return workqueue.NewTypedMaxOfRateLimiter(
		// 指数退避上限设为一分钟，保证命名空间删除至少每分钟重试一次。
		workqueue.NewTypedItemExponentialFailureRateLimiter[string](5*time.Millisecond, 60*time.Second),
		// 10 qps、桶容量 100。这里只影响整体重试速度，不针对单个条目限速。
		&workqueue.TypedBucketRateLimiter[string]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

// enqueueNamespace 将需要处理的命名空间加入控制器工作队列。
// obj 可以是 *v1.Namespace，也可以是 DeletionFinalStateUnknown 类型的对象。
// 只有带有删除时间戳的命名空间才会入队，且会延迟一段时间处理。
func (nm *NamespaceController) enqueueNamespace(ctx context.Context, obj interface{}) {
	// 借助通用 KeyFunc 提取键，兼容 informer 传入的墓碑对象。
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Couldn't get key for object", "object", obj)
		return
	}

	namespace := obj.(*v1.Namespace)
	// 尚未进入删除流程的命名空间无需清理，直接跳过，避免无意义的处理。
	if namespace.DeletionTimestamp == nil || namespace.DeletionTimestamp.IsZero() {
		return
	}

	// 延迟处理删除事件，给 HA apiserver 观察命名空间删除、
	// 以及 HA etcd 观察命名空间内最后一刻对象创建留出时间窗口，
	// 确保清理开始时状态已经收敛，不会遗漏对象。
	nm.queue.AddAfter(key, namespaceDeletionGracePeriod)
}

// worker 持续处理命名空间对象队列。
// 每个命名空间在队列中至多出现一次，系统保证不会有两个 worker
// 同时处理同一个命名空间，从而避免并发清理同一命名空间造成冲突。
func (nm *NamespaceController) worker(ctx context.Context) {
	workFunc := func(ctx context.Context) bool {
		key, quit := nm.queue.Get()
		if quit {
			// 队列已关闭，通知外层循环退出。
			return true
		}
		// 无论处理成功与否都要标记完成，否则该条目会一直占用队列。
		defer nm.queue.Done(key)

		err := nm.syncNamespaceFromKey(ctx, key)
		if err == nil {
			// 处理成功，清除该键的失败计数，避免后续重试受历史失败影响。
			nm.queue.Forget(key)
			return false
		}

		if estimate, ok := err.(*deletion.ResourcesRemainingError); ok {
			// 删除器给出了剩余资源数的估计，据此折半估算等待时间，
			// 让清理进度较快时更快重试，而不必走通用限速退避。
			t := estimate.Estimate/2 + 1
			klog.FromContext(ctx).V(4).Info("Content remaining in namespace", "namespace", key, "waitSeconds", t)
			nm.queue.AddAfter(key, time.Duration(t)*time.Second)
		} else {
			// 非资源残留类错误直接按限速重新入队，无需等待整个 resync 周期。
			nm.queue.AddRateLimited(key)
			utilruntime.HandleErrorWithContext(ctx, err, "Deletion of namespace failed", "namespace", key)
		}
		return false
	}
	// 持续取任务处理，直到队列关闭（workFunc 返回 quit）。
	for {
		quit := workFunc(ctx)

		if quit {
			return
		}
	}
}

// syncNamespaceFromKey 在本地存储中查找指定键的命名空间并对其进行同步：
// 找到则调用删除器清理命名空间内容，找不到（已删除）则视为处理完成。
func (nm *NamespaceController) syncNamespaceFromKey(ctx context.Context, key string) (err error) {
	startTime := time.Now()
	logger := klog.FromContext(ctx)
	// 用 defer 统一记录同步耗时，便于观测清理性能。
	defer func() {
		logger.V(4).Info("Finished syncing namespace", "namespace", key, "duration", time.Since(startTime))
	}()

	namespace, err := nm.lister.Get(key)
	if errors.IsNotFound(err) {
		// 命名空间已彻底删除，无需继续清理，直接视为成功。
		logger.Info("Namespace has been deleted", "namespace", key)
		return nil
	}
	if err != nil {
		// 缓存读取失败是暂时性错误，返回后由 worker 重新入队重试。
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to retrieve namespace from store", "namespace", key)
		return err
	}
	// 实际的内容清理与 finalizer 移除由删除器完成。
	return nm.namespacedResourcesDeleter.Delete(ctx, namespace.Name)
}

// Run 启动命名空间控制器，等待缓存同步后以指定数量的 worker 持续观察系统状态，
// 直到 ctx 被取消（收到关闭信号）才退出。
func (nm *NamespaceController) Run(ctx context.Context, workers int) {
	// 兜底捕获 worker 中的 panic，避免单个异常导致整个进程崩溃。
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("Starting namespace controller")

	var wg sync.WaitGroup
	// 退出时先关闭队列让 worker 停下，再等待所有 worker 真正结束，
	// 防止仍有任务在处理时进程提前退出。
	defer func() {
		logger.Info("Shutting down namespace controller")
		nm.queue.ShutDown()
		wg.Wait()
	}()

	// 缓存未同步前不启动 worker，否则会基于不完整的数据做清理决策。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, nm.listerSynced) {
		return
	}

	logger.V(5).Info("Starting workers of namespace controller")

	// 启动多个 worker 并发处理，队列会保证同一命名空间只被一个 worker 处理。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, nm.worker, time.Second)
		})
	}
	<-ctx.Done()
}
