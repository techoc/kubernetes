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

// Package main 演示如何用 client-go 的 informer 缓存与 workqueue 编写一个最小控制器：
// informer 负责把 Pod 事件转换为队列中的 key，worker 只依据本地缓存处理 key。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// Controller 演示如何用 client-go 实现控制器。
type Controller struct {
	// indexer 是 informer 维护的本地缓存视图，worker 只读它而不直接访问 API server。
	indexer cache.Indexer
	// queue 以 namespace/name 为 key，保证同一对象不会被两个 worker 并发处理。
	queue workqueue.TypedRateLimitingInterface[string]
	// informer 是事件来源；它必须在处理队列之前完成首次同步。
	informer cache.Controller
}

// NewController 创建一个 Controller。
func NewController(queue workqueue.TypedRateLimitingInterface[string], indexer cache.Indexer, informer cache.Controller) *Controller {
	return &Controller{
		informer: informer,
		indexer:  indexer,
		queue:    queue,
	}
}

// processNextItem 取出并处理一个 key；返回 false 表示队列已关闭、worker 应退出。
func (c *Controller) processNextItem(logger klog.Logger) bool {
	// 阻塞等待工作队列中出现新 key。
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// 通知队列该 key 已处理完毕，解除对其他 worker 的阻塞。这样可安全并行：
	// 同一 key 对应的 Pod 永远不会被两个 worker 同时处理。
	defer c.queue.Done(key)

	// 调用包含业务逻辑的方法。
	err := c.syncToStdout(key)
	// 业务逻辑执行出错时按策略处理。
	c.handleErr(logger, err, key)
	return true
}

// syncToStdout 是控制器的业务逻辑：这里只把 Pod 信息打印到 stdout。出错时只需返回错误，
// 重试逻辑不应混进业务逻辑。
func (c *Controller) syncToStdout(key string) error {
	// 读取本地缓存而不是 API server：缓存可能比触发入队时更新，这正是期望的行为。
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		return fmt.Errorf("fetching object with key %s from store failed: %w", key, err)
	}

	if !exists {
		// 下面会先往缓存里放一个 Pod，以便观察到一次删除事件。
		fmt.Printf("Pod %s does not exist anymore\n", key)
	} else {
		// 若持有依赖具体实例的本地资源，还需检查 UID，以识别同名 Pod 被重建的情况。
		fmt.Printf("Sync/Add/Update for Pod %s\n", obj.(*v1.Pod).GetName())
	}
	return nil
}

// handleErr 检查错误并确保稍后重试。
func (c *Controller) handleErr(logger klog.Logger, err error, key string) {
	if err == nil {
		// 每次同步成功后清除该 key 的限流重试历史，避免陈旧的失败记录拖慢后续处理。
		c.queue.Forget(key)
		return
	}

	// 该控制器出错最多重试 5 次，之后放弃。
	if c.queue.NumRequeues(key) < 5 {
		logger.Info("Syncing failed, will retry", "pod", key, "err", err)

		// 按限流策略重新入队，队列会结合限流器与重试历史决定何时再次处理。
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// 多次重试仍失败时，向外部报告该 key 处理失败。
	runtime.HandleErrorWithLogger(logger, err, "Dropping pod out of the queue", "pod", key)
}

// Run 启动监听与同步。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer runtime.HandleCrashWithContext(ctx)
	logger := klog.FromContext(ctx)

	// 结束时通知 worker 停止。
	defer c.queue.ShutDown()
	logger.Info("Starting Pod controller")
	defer logger.Info("Stopping Pod controller")

	go c.informer.RunWithContext(ctx)

	// 处理队列前先等待所有相关缓存完成同步。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-ctx.Done()
}

// runWorker 持续消费队列，直到 processNextItem 报告队列已关闭。
func (c *Controller) runWorker(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for c.processNextItem(logger) {
	}
}

func main() {
	var kubeconfig string
	var master string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.Parse()

	// 更完整的示例可能允许配置不同日志后端；这里直接使用 klog 默认值。
	logger := klog.Background()

	// 创建连接配置。
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		logger.Error(err, "Building Kubernetes client config failed")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	// 创建 clientset。
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error(err, "Building Kubernetes client failed")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	// ctx 目前仅用于取消 controller；示例没有安装信号处理，因此实际不会触发取消。
	ctx := context.Background()

	// 创建 Pod Informer。
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "pods", v1.NamespaceDefault, fields.Everything())

	// 创建工作队列。
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())

	// 借助 informer 把工作队列与缓存绑定：缓存每次更新都会把 Pod key 入队。
	// 注意最终处理 key 时，缓存中的 Pod 可能比触发入队时的版本更新。
	indexer, informer := cache.NewIndexerInformer(podListWatcher, &v1.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer 使用 delta 队列，删除事件需用该函数取出 key。
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}, cache.Indexers{})

	controller := NewController(queue, indexer, informer)

	// 先预热缓存以完成初次同步：假设上次运行时已知 Pod "mypod"，把它加入缓存。
	// 若该 Pod 已不存在，缓存同步后控制器会收到删除通知。
	// 手工放入一个缓存项，模拟上次运行遗留的状态。
	indexer.Add(&v1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      "mypod",
			Namespace: v1.NamespaceDefault,
		},
	})

	// 启动控制器。
	cancelCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("time to stop because main has completed"))
	go controller.Run(cancelCtx, 1)

	// 永久阻塞。
	select {}
}
