/*
Copyright 2020 The Kubernetes Authors.

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

package endpointslicemirroring

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/endpointslicemirroring/metrics"
	endpointslicepkg "k8s.io/kubernetes/pkg/controller/util/endpointslice"
)

const (
	// maxRetries 是 Endpoints 资源在被丢弃出队列前的最大重试次数。
	// 任何同步错误（如创建或更新 EndpointSlice 失败）都会触发重试。
	// 当前限速器按 1s*2^(numRetries-1) 退避，最长 100s。
	// 以下数字表示同一 Endpoints 资源相邻两次入队之间的退避间隔：
	// 1s、2s、4s、8s、16s、32s、64s、100s（上限）。
	//
	maxRetries = 15

	// defaultSyncBackOff 是 syncEndpoints 调用的默认退避时长。
	defaultSyncBackOff = 1 * time.Second
	// maxSyncBackOff 是 syncEndpoints 调用的最大退避时长。
	maxSyncBackOff = 100 * time.Second

	// ControllerName 是与 LabelManagedBy 配合使用的唯一值，用于标识管理该 EndpointSlice 的组件。
	ControllerName = "endpointslicemirroring-controller.k8s.io"
)

// NewController 创建并初始化一个新的 Controller。
// 它注册 Endpoints、EndpointSlice 和 Service 的事件处理器，并初始化协调器与限速队列。
func NewController(ctx context.Context, endpointsInformer coreinformers.EndpointsInformer,
	endpointSliceInformer discoveryinformers.EndpointSliceInformer,
	serviceInformer coreinformers.ServiceInformer,
	maxEndpointsPerSubset int32,
	client clientset.Interface,
	endpointUpdatesBatchPeriod time.Duration,
) *Controller {
	logger := klog.FromContext(ctx)
	// 事件广播器，用于把控制器事件记录到 API Server。
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "endpoint-slice-mirroring-controller"})

	metrics.RegisterMetrics()

	c := &Controller{
		client: client,
		// 类似 DefaultControllerRateLimiter，但默认退避大得多（1s 对比 5ms）。
		// 该控制器处理的事件可能引起大量 EndpointSlice 变更，
		// 因此更保守的退避能避免对 API Server 造成冲击。
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(workqueue.NewTypedMaxOfRateLimiter(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](defaultSyncBackOff, maxSyncBackOff),
			// 10 qps、桶容量 100；只影响整体重试速度，不针对单个条目。
			&workqueue.TypedBucketRateLimiter[string]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
		),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "endpoint_slice_mirroring",
			},
		),
		workerLoopPeriod: time.Second,
	}

	endpointsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.onEndpointsAdd(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.onEndpointsUpdate(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.onEndpointsDelete(logger, obj)
		},
	})
	c.endpointsLister = endpointsInformer.Lister()
	c.endpointsSynced = endpointsInformer.Informer().HasSynced

	endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.onEndpointSliceAdd,
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.onEndpointSliceUpdate(logger, oldObj, newObj)
		},
		DeleteFunc: c.onEndpointSliceDelete,
	})

	c.endpointSliceLister = endpointSliceInformer.Lister()
	c.endpointSlicesSynced = endpointSliceInformer.Informer().HasSynced
	c.endpointSliceTracker = endpointsliceutil.NewEndpointSliceTracker()

	c.serviceLister = serviceInformer.Lister()
	c.servicesSynced = serviceInformer.Informer().HasSynced
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onServiceAdd,
		UpdateFunc: c.onServiceUpdate,
		DeleteFunc: c.onServiceDelete,
	})

	c.maxEndpointsPerSubset = maxEndpointsPerSubset

	c.reconciler = &reconciler{
		client:                c.client,
		maxEndpointsPerSubset: c.maxEndpointsPerSubset,
		endpointSliceTracker:  c.endpointSliceTracker,
		metricsCache:          metrics.NewCache(maxEndpointsPerSubset),
		eventRecorder:         recorder,
	}

	c.eventBroadcaster = broadcaster
	c.eventRecorder = recorder

	c.endpointUpdatesBatchPeriod = endpointUpdatesBatchPeriod

	return c
}

// Controller 管理基于 selector 的 Service 对应的 EndpointSlice 镜像。
type Controller struct {
	client           clientset.Interface
	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	// endpointsLister 从共享 informer 缓存中列出/获取 Endpoints。
	endpointsLister corelisters.EndpointsLister
	// endpointsSynced 表示 Endpoints informer 至少完成过一次同步。
	// 作为结构体字段是为了在测试中注入。
	endpointsSynced cache.InformerSynced

	// endpointSliceLister 从共享 informer 缓存中列出/获取 EndpointSlice。
	endpointSliceLister discoverylisters.EndpointSliceLister
	// endpointSlicesSynced 表示 EndpointSlice informer 至少完成过一次同步。
	// 作为结构体字段是为了在测试中注入。
	endpointSlicesSynced cache.InformerSynced

	// endpointSliceTracker 跟踪每个 Endpoints 资源所期望的 EndpointSlice 列表及其资源版本，
	// 用于判断缓存中的 EndpointSlice 是否已经过期。
	endpointSliceTracker *endpointsliceutil.EndpointSliceTracker

	// serviceLister 从共享 informer 缓存中列出/获取 Service。
	serviceLister corelisters.ServiceLister
	// servicesSynced 表示 Service informer 至少完成过一次同步。
	// 作为结构体字段是为了在测试中注入。
	servicesSynced cache.InformerSynced

	// reconciler 负责执行 EndpointSlice 的实际协调逻辑。
	reconciler *reconciler

	// queue 保存需要同步的 Endpoints 键。这里不适合用 channel：
	// channel 会让地址多的 Endpoints 被过于频繁地处理，也会让重复入队的同一对象被多次处理；
	// workqueue 则能对同一键去重并按限速重试。
	queue workqueue.TypedRateLimitingInterface[string]

	// maxEndpointsPerSubset 是单个 EndpointSubset 可加入 EndpointSlice 的最大端点数。
	maxEndpointsPerSubset int32

	// workerLoopPeriod 是 worker 两次运行之间的间隔。
	workerLoopPeriod time.Duration

	// endpointUpdatesBatchPeriod 是 EndpointSlice 变更触发的 Endpoints 同步所附加的人为延迟，
	// 用于对多次变更做批处理，减少 EndpointSlice 更新总次数。
	endpointUpdatesBatchPeriod time.Duration
}

// Run 启动控制器，直到 ctx 结束才返回。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线：日志与写入 API Server 的事件接收端。
	c.eventBroadcaster.StartLogging(klog.Infof)
	c.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.client.CoreV1().Events("")})
	defer c.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting EndpointSliceMirroring controller")

	var wg sync.WaitGroup
	// 关闭时先关闭队列，再等待所有 worker 退出。
	defer func() {
		logger.Info("Shutting down EndpointSliceMirroring controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	// 等三类 informer 缓存都同步完成后再启动 worker，避免读到不完整数据。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.endpointsSynced, c.endpointSlicesSynced, c.servicesSynced) {
		return
	}

	logger.V(2).Info("Starting worker threads", "total", workers)

	// 启动多个 worker 并发处理；workqueue 保证同一个键不会被并发处理。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.worker, c.workerLoopPeriod)
		})
	}
	<-ctx.Done()
}

// worker 运行一个工作线程，不断出队、处理并标记条目完成。
// 可以并行运行任意多个 worker；workqueue 保证同一个 Service 不会被同时处理。
func (c *Controller) worker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 Endpoints 键，并按处理结果更新重试计数。
// 返回 false 表示队列已关闭，worker 应退出。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	cKey, quit := c.queue.Get()
	if quit {
		return false
	}
	// 保证每次 Get 都有对应的 Done，否则队列条目会一直处于处理中状态。
	defer c.queue.Done(cKey)

	err := c.syncEndpoints(ctx, cKey)
	c.handleErr(klog.FromContext(ctx), err, cKey)

	return true
}

// handleErr 处理同步错误：成功则清除重试计数，未超过上限则限速重试，超过上限则丢弃。
func (c *Controller) handleErr(logger klog.Logger, err error, key string) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	// 重试次数未达上限，按限速队列的退避策略重新入队。
	if c.queue.NumRequeues(key) < maxRetries {
		logger.Info("Error mirroring EndpointSlices for Endpoints, retrying", "key", key, "err", err)
		c.queue.AddRateLimited(key)
		return
	}

	// 重试预算耗尽：清除计数并丢弃该键，避免坏对象无限阻塞队列。
	logger.Info("Retry budget exceeded, dropping Endpoints out of the queue", "key", key, "err", err)
	c.queue.Forget(key)
	utilruntime.HandleError(err)
}

// syncEndpoints 对一个 Endpoints 资源执行一次完整同步：必要时创建、更新或删除镜像 EndpointSlice。
func (c *Controller) syncEndpoints(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	// 用 defer 统计每次同步耗时，覆盖所有返回路径。
	defer func() {
		syncDuration := float64(time.Since(startTime).Milliseconds()) / 1000
		metrics.EndpointsSyncDuration.WithLabelValues().Observe(syncDuration)
		logger.V(4).Info("Finished syncing EndpointSlices for Endpoints", "key", key, "elapsedTime", time.Since(startTime))
	}()

	logger.V(4).Info("syncEndpoints", "key", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	endpoints, err := c.endpointsLister.Endpoints(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Endpoints 已删除，清理其所有镜像 EndpointSlice。
			logger.V(4).Info("Endpoints not found, cleaning up any mirrored EndpointSlices", "endpoints", klog.KRef(namespace, name))
			c.endpointSliceTracker.DeleteService(namespace, name)
			return c.deleteMirroredSlices(ctx, namespace, name)
		}
		return err
	}

	// 带有跳过镜像标签或 leader election 注解的 Endpoints 不应被镜像。
	if !c.shouldMirror(endpoints) {
		logger.V(4).Info("Endpoints should not be mirrored, cleaning up any mirrored EndpointSlices", "endpoints", klog.KRef(namespace, name))
		c.endpointSliceTracker.DeleteService(namespace, name)
		return c.deleteMirroredSlices(ctx, namespace, name)
	}

	svc, err := c.serviceLister.Services(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// 对应 Service 已删除，清理镜像 EndpointSlice。
			logger.V(4).Info("Service not found, cleaning up any mirrored EndpointSlices", "service", klog.KRef(namespace, name))
			c.endpointSliceTracker.DeleteService(namespace, name)
			return c.deleteMirroredSlices(ctx, namespace, name)
		}
		return err
	}

	// Service 一旦设置了 selector，就应由 EndpointSlice 控制器接管，清理本控制器创建的镜像。
	if svc.Spec.Selector != nil {
		logger.V(4).Info("Service now has selector, cleaning up any mirrored EndpointSlices", "service", klog.KRef(namespace, name))
		c.endpointSliceTracker.DeleteService(namespace, name)
		return c.deleteMirroredSlices(ctx, namespace, name)
	}

	endpointSlices, err := endpointSlicesMirroredForService(c.endpointSliceLister, namespace, name)
	if err != nil {
		return err
	}

	// tracker 记录的期望版本与缓存不一致，说明 informer 缓存过期，返回特殊错误触发重试。
	if c.endpointSliceTracker.StaleSlices(svc, endpointSlices) {
		return endpointslicepkg.NewStaleInformerCache("EndpointSlice informer cache is out of date")
	}

	err = c.reconciler.reconcile(ctx, endpoints, endpointSlices)
	if err != nil {
		return err
	}

	return nil
}

// queueEndpoints 把 Endpoints 资源对应的键加入工作队列等待处理。
func (c *Controller) queueEndpoints(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v (type %T): %v", obj, obj, err))
		return
	}

	c.queue.Add(key)
}

// shouldMirror 判断一个 Endpoints 资源是否应由本控制器镜像。
// 以下情况返回 false：
// - Endpoints 为 nil；
// - 带有跳过镜像的标签；
// - 带有 leader election 注解。
// 本函数不检查是否存在 selector 为空的对应 Service，该检查需单独进行。
func (c *Controller) shouldMirror(endpoints *v1.Endpoints) bool {
	if endpoints == nil || skipMirror(endpoints.Labels) || hasLeaderElection(endpoints.Annotations) {
		return false
	}

	return true
}

// onServiceAdd 在 selector 为空的 Service 创建时，把对应 Endpoints 加入队列。
func (c *Controller) onServiceAdd(obj interface{}) {
	service := obj.(*v1.Service)
	if service == nil {
		utilruntime.HandleError(fmt.Errorf("onServiceAdd() expected type v1.Service, got %T", obj))
		return
	}
	// 只有无 selector 的 Service 才需要镜像 Endpoints。
	if service.Spec.Selector == nil {
		c.queueEndpoints(obj)
	}
}

// onServiceUpdate 在 Service 的 selector 有无发生变化时，把对应 Endpoints 加入队列。
func (c *Controller) onServiceUpdate(prevObj, obj interface{}) {
	service := obj.(*v1.Service)
	prevService := prevObj.(*v1.Service)
	if service == nil || prevService == nil {
		utilruntime.HandleError(fmt.Errorf("onServiceUpdate() expected type v1.Service, got %T, %T", prevObj, obj))
		return
	}
	// selector 从无到有或反之，都意味着镜像归属发生变化，需要同步清理或创建。
	if (service.Spec.Selector == nil) != (prevService.Spec.Selector == nil) {
		c.queueEndpoints(obj)
	}
}

// onServiceDelete 在 selector 为空的 Service 删除时，把对应 Endpoints 加入队列以清理镜像。
func (c *Controller) onServiceDelete(obj interface{}) {
	service := getServiceFromDeleteAction(obj)
	if service == nil {
		utilruntime.HandleError(fmt.Errorf("onServiceDelete() expected type v1.Service, got %T", obj))
		return
	}
	if service.Spec.Selector == nil {
		c.queueEndpoints(obj)
	}
}

// onEndpointsAdd 在需要镜像的 Endpoints 创建时将其加入队列。
func (c *Controller) onEndpointsAdd(logger klog.Logger, obj interface{}) {
	endpoints := obj.(*v1.Endpoints)
	if endpoints == nil {
		utilruntime.HandleError(fmt.Errorf("onEndpointsAdd() expected type v1.Endpoints, got %T", obj))
		return
	}
	if !c.shouldMirror(endpoints) {
		logger.V(5).Info("Skipping mirroring", "endpoints", klog.KObj(endpoints))
		return
	}
	c.queueEndpoints(obj)
}

// onEndpointsUpdate 在 Endpoints 更新时，只要新旧任一版本需要镜像，就把对应 Endpoints 加入队列。
func (c *Controller) onEndpointsUpdate(logger klog.Logger, prevObj, obj interface{}) {
	endpoints := obj.(*v1.Endpoints)
	prevEndpoints := prevObj.(*v1.Endpoints)
	if endpoints == nil || prevEndpoints == nil {
		utilruntime.HandleError(fmt.Errorf("onEndpointsUpdate() expected type v1.Endpoints, got %T, %T", prevObj, obj))
		return
	}
	// 新旧都不需要镜像时跳过；任一需要镜像则入队，以便处理镜像状态的切换。
	if !c.shouldMirror(endpoints) && !c.shouldMirror(prevEndpoints) {
		logger.V(5).Info("Skipping mirroring", "endpoints", klog.KObj(endpoints))
		return
	}
	c.queueEndpoints(obj)
}

// onEndpointsDelete 在需要镜像的 Endpoints 删除时将其加入队列以清理镜像。
func (c *Controller) onEndpointsDelete(logger klog.Logger, obj interface{}) {
	endpoints := getEndpointsFromDeleteAction(obj)
	if endpoints == nil {
		utilruntime.HandleError(fmt.Errorf("onEndpointsDelete() expected type v1.Endpoints, got %T", obj))
		return
	}
	if !c.shouldMirror(endpoints) {
		logger.V(5).Info("Skipping mirroring", "endpoints", klog.KObj(endpoints))
		return
	}
	c.queueEndpoints(obj)
}

// onEndpointSliceAdd 在 EndpointSlice 的资源版本与 tracker 中期望的不一致时，
// 把对应的 Endpoints 加入队列同步。
func (c *Controller) onEndpointSliceAdd(obj interface{}) {
	endpointSlice := obj.(*discovery.EndpointSlice)
	if endpointSlice == nil {
		utilruntime.HandleError(fmt.Errorf("onEndpointSliceAdd() expected type discovery.EndpointSlice, got %T", obj))
		return
	}
	// 只处理由本控制器管理的 EndpointSlice，避免被其他控制器的对象触发无谓同步。
	if managedByController(endpointSlice) && c.endpointSliceTracker.ShouldSync(endpointSlice) {
		c.queueEndpointsForEndpointSlice(endpointSlice)
	}
}

// onEndpointSliceUpdate 在 EndpointSlice 版本与期望不一致，或其 managed-by 值变为/不再是本控制器时，
// 把对应的 Endpoints 加入队列同步。
func (c *Controller) onEndpointSliceUpdate(logger klog.Logger, prevObj, obj interface{}) {
	endpointSlice := obj.(*discovery.EndpointSlice)
	prevEndpointSlice := prevObj.(*discovery.EndpointSlice)
	if endpointSlice == nil || prevEndpointSlice == nil {
		utilruntime.HandleError(fmt.Errorf("onEndpointSliceUpdated() expected type discovery.EndpointSlice, got %T, %T", prevObj, obj))
		return
	}
	// 仅修改 label 不会改变 generation；虽然本控制器从不改 LabelServiceName，但用户可能改。
	// 因此显式比较新旧 label，确保这种变更也能触发同步。
	svcName := endpointSlice.Labels[discovery.LabelServiceName]
	prevSvcName := prevEndpointSlice.Labels[discovery.LabelServiceName]
	if svcName != prevSvcName {
		logger.Info("LabelServiceName changed", "labelServiceName", discovery.LabelServiceName, "oldName", prevSvcName, "newName", svcName, "endpointSlice", klog.KObj(endpointSlice))
		c.queueEndpointsForEndpointSlice(endpointSlice)
		c.queueEndpointsForEndpointSlice(prevEndpointSlice)
		return
	}
	if managedByChanged(prevEndpointSlice, endpointSlice) || (managedByController(endpointSlice) && c.endpointSliceTracker.ShouldSync(endpointSlice)) {
		c.queueEndpointsForEndpointSlice(endpointSlice)
	}
}

// onEndpointSliceDelete 在本控制器管理的 EndpointSlice 被意外删除时，
// 把对应的 Endpoints 加入队列以触发重建。
func (c *Controller) onEndpointSliceDelete(obj interface{}) {
	endpointSlice := getEndpointSliceFromDeleteAction(obj)
	if endpointSlice == nil {
		utilruntime.HandleError(fmt.Errorf("onEndpointSliceDelete() expected type discovery.EndpointSlice, got %T", obj))
		return
	}
	if managedByController(endpointSlice) && c.endpointSliceTracker.Has(endpointSlice) {
		// 返回 false 表示这次删除不在预期内（并非本控制器发起），需要重新同步以重建。
		if !c.endpointSliceTracker.HandleDeletion(endpointSlice) {
			c.queueEndpointsForEndpointSlice(endpointSlice)
		}
	}
}

// queueEndpointsForEndpointSlice 把给定 EndpointSlice 对应的 Endpoints 延迟加入队列。
// 延迟由 endpointUpdatesBatchPeriod 控制，以合并短时间内多次 EndpointSlice 变更。
func (c *Controller) queueEndpointsForEndpointSlice(endpointSlice *discovery.EndpointSlice) {
	key, err := endpointsControllerKey(endpointSlice)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for EndpointSlice %+v (type %T): %v", endpointSlice, endpointSlice, err))
		return
	}

	// 延迟入队，给同一批次的多次变更留出合并窗口。
	c.queue.AddAfter(key, c.endpointUpdatesBatchPeriod)
}

// deleteMirroredSlices 删除指定命名空间和名称下已被镜像的所有 EndpointSlice。
func (c *Controller) deleteMirroredSlices(ctx context.Context, namespace, name string) error {
	endpointSlices, err := endpointSlicesMirroredForService(c.endpointSliceLister, namespace, name)
	if err != nil {
		return err
	}

	// 先清除 tracker 中该 Service 的记录，避免后续同步误判为版本过期。
	c.endpointSliceTracker.DeleteService(namespace, name)
	return c.reconciler.deleteEndpoints(ctx, namespace, name, endpointSlices)
}

// endpointSlicesMirroredForService 返回由本控制器为指定 Service 镜像的所有 EndpointSlice。
// 通过 Service 名称标签与 managed-by 标签联合筛选，避免误删其他控制器的对象。
func endpointSlicesMirroredForService(endpointSliceLister discoverylisters.EndpointSliceLister, namespace, name string) ([]*discovery.EndpointSlice, error) {
	esLabelSelector := labels.Set(map[string]string{
		discovery.LabelServiceName: name,
		discovery.LabelManagedBy:   ControllerName,
	}).AsSelectorPreValidated()
	return endpointSliceLister.EndpointSlices(namespace).List(esLabelSelector)
}
