/*
Copyright 2019 The Kubernetes Authors.

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

package endpointslice

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
	utilfeature "k8s.io/apiserver/pkg/util/feature"
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
	endpointslicerec "k8s.io/endpointslice"
	endpointslicemetrics "k8s.io/endpointslice/metrics"
	"k8s.io/endpointslice/topologycache"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	endpointslicepkg "k8s.io/kubernetes/pkg/controller/util/endpointslice"
	"k8s.io/kubernetes/pkg/features"
)

// 以下常量定义控制器的重试策略、同步延迟、退避周期与标识值。
const (
	// maxRetries 是 Service 在出队前允许重试的次数。任何同步错误（例如创建或更新 EndpointSlice 失败）
	// 都可能触发重试。以当前限速器（1s*2^(numRetries-1)）为例，连续入队的延迟依次为：
	//
	// 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s, 1000s（上限）
	// 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s, 1000s (max)
	maxRetries = 15

	// endpointSliceChangeMinSyncDelay 表示 EndpointSlice 变更后将 syncService 入队的最小延迟。
	// 若 endpointUpdatesBatchPeriod 更大，则使用后者。该延迟有助于合并对多个 EndpointSlice
	// 的连续变更，避免逐个触发同步。
	endpointSliceChangeMinSyncDelay = 1 * time.Second

	// defaultSyncBackOff 是 syncService 调用的默认退避周期。
	defaultSyncBackOff = 1 * time.Second
	// maxSyncBackOff 是 syncService 调用的最大退避周期。
	maxSyncBackOff = 1000 * time.Second

	// ControllerName 是与 LabelManagedBy 配合使用的唯一标识值，
	// 用于标明某个 EndpointSlice 由哪个组件管理。
	ControllerName = "endpointslice-controller.k8s.io"

	// topologyQueueItemKey 是 topologyQueue 中所有项共用固定键，因为拓扑检查是全局性的。
	topologyQueueItemKey = "topologyQueueItemKey"
)

// NewController 创建并初始化一个 Controller：注册各 informer 的事件处理器、
// 构造工作队列，并初始化 tracker、拓扑缓存与 EndpointSlice 协调器。
func NewController(ctx context.Context, podInformer coreinformers.PodInformer,
	serviceInformer coreinformers.ServiceInformer,
	nodeInformer coreinformers.NodeInformer,
	endpointSliceInformer discoveryinformers.EndpointSliceInformer,
	maxEndpointsPerSlice int32,
	client clientset.Interface,
	endpointUpdatesBatchPeriod time.Duration,
) *Controller {
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "endpoint-slice-controller"})

	endpointslicemetrics.RegisterMetrics()

	c := &Controller{
		client: client,
		// 与 DefaultControllerRateLimiter 类似，但默认退避显著更大（1s 对比 5ms）。
		// 本控制器处理的事件可能引发大规模 EndpointSlice 变更（例如 Service 或 Deployment 更新），
		// 更大的退避可以避免控制器对 API Server 造成过大压力。
		serviceQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedMaxOfRateLimiter(
				workqueue.NewTypedItemExponentialFailureRateLimiter[string](defaultSyncBackOff, maxSyncBackOff),
				// 10 qps，桶大小 100。仅用于限制重试速率，
				// 且是整体因子（不是针对每个队列项）。
				&workqueue.TypedBucketRateLimiter[string]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "endpoint_slice",
			},
		),
		podQueue:         workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*endpointsliceutil.PodProjectionKey]()),
		topologyQueue:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		workerLoopPeriod: time.Second,
	}

	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.onServiceUpdate,
		UpdateFunc: func(old, cur interface{}) {
			c.onServiceUpdate(cur)
		},
		DeleteFunc: c.onServiceDelete,
	})
	c.serviceLister = serviceInformer.Lister()
	c.servicesSynced = serviceInformer.Informer().HasSynced

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.onPodUpdate(nil, obj) },
		UpdateFunc: c.onPodUpdate,
		DeleteFunc: func(obj interface{}) { c.onPodUpdate(obj, nil) },
	})
	c.podLister = podInformer.Lister()
	c.podsSynced = podInformer.Informer().HasSynced

	c.nodeLister = nodeInformer.Lister()
	c.nodesSynced = nodeInformer.Informer().HasSynced

	logger := klog.FromContext(ctx)
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

	c.maxEndpointsPerSlice = maxEndpointsPerSlice

	c.triggerTimeTracker = endpointsliceutil.NewTriggerTimeTracker()

	c.eventBroadcaster = broadcaster
	c.eventRecorder = recorder

	c.endpointUpdatesBatchPeriod = endpointUpdatesBatchPeriod

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(_ interface{}) {
			c.addNode()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.updateNode(oldObj, newObj)
		},
		DeleteFunc: func(_ interface{}) {
			c.deleteNode()
		},
	})
	c.topologyCache = topologycache.NewTopologyCache()

	c.reconciler = endpointslicerec.NewReconciler(
		c.client,
		c.nodeLister,
		c.maxEndpointsPerSlice,
		c.endpointSliceTracker,
		c.topologyCache,
		c.eventRecorder,
		ControllerName,
		endpointslicerec.WithPreferSameTrafficDistributionEnabled(utilfeature.DefaultFeatureGate.Enabled(features.PreferSameTrafficDistribution)),
	)

	return c
}

// Controller 管理基于 selector 的 Service 对应的 EndpointSlice，
// 通过多个队列分别处理 Service、Pod 与拓扑变化，并维护缓存与协调所需的各类 tracker。
type Controller struct {
	client           clientset.Interface
	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	// serviceLister 用于 list/get Service，数据来自传给 NewController 的共享 informer。
	serviceLister corelisters.ServiceLister
	// servicesSynced 在 Service 共享 informer 至少完成一次同步后返回 true。
	// 作为结构体字段是为了便于在测试中注入。
	servicesSynced cache.InformerSynced

	// podLister 用于 list/get Pod，数据来自传给 NewController 的共享 informer。
	podLister corelisters.PodLister
	// podsSynced 在 Pod 共享 informer 至少完成一次同步后返回 true。
	// 作为结构体字段是为了便于在测试中注入。
	podsSynced cache.InformerSynced

	// endpointSliceLister 用于 list/get EndpointSlice，数据来自传给 NewController 的共享 informer。
	endpointSliceLister discoverylisters.EndpointSliceLister
	// endpointSlicesSynced 在 EndpointSlice 共享 informer 至少完成一次同步后返回 true。
	// 作为结构体字段是为了便于在测试中注入。
	endpointSlicesSynced cache.InformerSynced
	// endpointSliceTracker 记录每个 Service 预期的 EndpointSlice 列表及其资源版本，
	// 用于判断缓存中的 EndpointSlice 是否已过期。
	endpointSliceTracker *endpointsliceutil.EndpointSliceTracker

	// nodeLister 用于 list/get Node，数据来自传给 NewController 的共享 informer。
	nodeLister corelisters.NodeLister
	// nodesSynced 在 Node 共享 informer 至少完成一次同步后返回 true。
	// 作为结构体字段是为了便于在测试中注入。
	nodesSynced cache.InformerSynced

	// reconciler 是执行 EndpointSlice 实际协调的工具。
	reconciler *endpointslicerec.Reconciler

	// triggerTimeTracker 用于计算并导出 EndpointsLastChangeTriggerTime 注解，
	// 记录导致端点变更的最早触发时间。
	triggerTimeTracker *endpointsliceutil.TriggerTimeTracker

	// serviceQueue 保存需要更新的 Service。此处不适合使用 channel：
	// channel 会让 Pod 数量多的 Service 获得远高于 Pod 少的 Service 的处理频率；
	// 而且同一个 Service 被多次插入时会被重复处理，造成不必要的同步。
	serviceQueue workqueue.TypedRateLimitingInterface[string]

	// podQueue 用于计算 pod->services 映射，并将匹配的 Service 推入 serviceQueue。
	// 当 Pod 所在 namespace 中存在大量 Service 时，这一操作开销较大，
	// 因为需要为每个 Service 逐一评估标签选择逻辑。
	podQueue workqueue.TypedRateLimitingInterface[*endpointsliceutil.PodProjectionKey]

	// topologyQueue 用于触发拓扑缓存更新及节点拓扑分布检查。
	topologyQueue workqueue.TypedRateLimitingInterface[string]

	// maxEndpointsPerSlice 指定单个 EndpointSlice 中允许加入的最大端点数。
	maxEndpointsPerSlice int32

	// workerLoopPeriod 是 worker 两次运行之间的间隔；worker 负责处理 Service 与 Pod 变更队列。
	workerLoopPeriod time.Duration

	// endpointUpdatesBatchPeriod 是 Pod 变更触发 Service 同步时人为加入的延迟，
	// 用于减少 EndpointSlice 的总体更新次数。
	endpointUpdatesBatchPeriod time.Duration

	// topologyCache 跟踪各可用区中节点与端点的分布，为实现拓扑感知提示（TopologyAwareHints）提供依据。
	topologyCache *topologycache.TopologyCache
}

// Run 启动事件广播器、等待各 informer 缓存同步，然后启动 Service、Pod 与拓扑队列的 worker，
// 直到 ctx 被取消才返回。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线：日志与事件落盘。
	c.eventBroadcaster.StartLogging(klog.Infof)
	c.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.client.CoreV1().Events("")})
	defer c.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting endpoint slice controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down endpoint slice controller")
		c.serviceQueue.ShutDown()
		c.podQueue.ShutDown()
		c.topologyQueue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.podsSynced, c.servicesSynced, c.endpointSlicesSynced, c.nodesSynced) {
		return
	}

	logger.V(2).Info("Starting service queue worker threads", "total", workers)
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.Until(func() { c.serviceQueueWorker(logger) }, c.workerLoopPeriod, ctx.Done())
		})
		wg.Go(func() {
			wait.Until(func() { c.podQueueWorker(logger) }, c.workerLoopPeriod, ctx.Done())
		})
	}

	logger.V(2).Info("Starting topology queue worker threads", "total", 1)
	wg.Go(func() {
		wait.Until(func() { c.topologyQueueWorker(logger) }, c.workerLoopPeriod, ctx.Done())
	})
	<-ctx.Done()
}

// serviceQueueWorker 运行一个工作线程，不断从队列中取出项、处理并标记完成。
// 可以并行运行任意数量的工作线程；workqueue 保证同一个 Service 不会被并发处理，
// 从而避免对同一 Service 的 EndpointSlice 产生竞争性的写操作。
func (c *Controller) serviceQueueWorker(logger klog.Logger) {
	for c.processNextServiceWorkItem(logger) {
	}
}

// processNextServiceWorkItem 取出下一个 Service 键，执行同步并按结果安排重试。
func (c *Controller) processNextServiceWorkItem(logger klog.Logger) bool {
	cKey, quit := c.serviceQueue.Get()
	if quit {
		return false
	}
	defer c.serviceQueue.Done(cKey)

	err := c.syncService(logger, cKey)
	c.handleErr(logger, err, cKey)

	return true
}

// topologyQueueWorker 持续从 topologyQueue 中取出并处理拓扑更新项。
func (c *Controller) topologyQueueWorker(logger klog.Logger) {
	for c.processNextTopologyWorkItem(logger) {
	}
}

// processNextTopologyWorkItem 取出拓扑项，执行节点拓扑分布检查。
func (c *Controller) processNextTopologyWorkItem(logger klog.Logger) bool {
	key, quit := c.topologyQueue.Get()
	if quit {
		return false
	}
	defer c.topologyQueue.Done(key)
	c.checkNodeTopologyDistribution(logger)
	return true
}

// handleErr 按重试预算处理 Service 同步错误：成功则清除计数，
// 未超过上限则限速重新入队，超过上限则丢弃并上报错误。
func (c *Controller) handleErr(logger klog.Logger, err error, key string) {
	trackSync(err)

	if err == nil {
		c.serviceQueue.Forget(key)
		return
	}

	if c.serviceQueue.NumRequeues(key) < maxRetries {
		logger.Info("Error syncing endpoint slices for service, retrying", "key", key, "err", err)
		c.serviceQueue.AddRateLimited(key)
		return
	}

	logger.Info("Retry budget exceeded, dropping service out of the queue", "key", key, "err", err)
	c.serviceQueue.Forget(key)
	utilruntime.HandleError(err)
}

// syncService 协调单个 Service 的 EndpointSlice：从各 informer 缓存中读取 Service、匹配的 Pod
// 与受控 EndpointSlice，校验缓存新鲜度后调用 reconciler 完成实际协调。
// 对于 ExternalName 或无 selector 的 Service 直接跳过，因为本控制器不为其生成端点。
func (c *Controller) syncService(logger klog.Logger, key string) error {
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing service endpoint slices", "key", key, "elapsedTime", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	service, err := c.serviceLister.Services(namespace).Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		c.triggerTimeTracker.DeleteService(namespace, name)
		c.reconciler.DeleteService(namespace, name)
		c.endpointSliceTracker.DeleteService(namespace, name)
		// Service 已删除，返回 nil 避免再次重试。
		return nil
	}

	if service.Spec.Type == v1.ServiceTypeExternalName {
		// ExternalName 类型的 Service 不从本控制器获得任何端点；
		// 参考：https://issues.k8s.io/105986
		return nil
	}

	if service.Spec.Selector == nil {
		// 没有 selector 的 Service 不会由本控制器创建 EndpointSlice；
		// 这类 Service 的 EndpointSlice 由用户通过 REST API 在带外创建。
		return nil
	}

	logger.V(5).Info("About to update endpoint slices for service", "key", key)

	podLabelSelector := labels.Set(service.Spec.Selector).AsSelectorPreValidated()
	pods, err := c.podLister.Pods(service.Namespace).List(podLabelSelector)
	if err != nil {
		// 数据来自本地缓存，理论上基本不会出现此错误；一旦出现则记录事件并重试。
		c.eventRecorder.Eventf(service, v1.EventTypeWarning, "FailedToListPods",
			"Error listing Pods for Service %s/%s: %v", service.Namespace, service.Name, err)
		return err
	}

	esLabelSelector := labels.Set(map[string]string{
		discovery.LabelServiceName: service.Name,
		discovery.LabelManagedBy:   c.reconciler.GetControllerName(),
	}).AsSelectorPreValidated()
	endpointSlices, err := c.endpointSliceLister.EndpointSlices(service.Namespace).List(esLabelSelector)

	if err != nil {
		// 数据来自本地缓存，理论上基本不会出现此错误；一旦出现则记录事件并重试。
		c.eventRecorder.Eventf(service, v1.EventTypeWarning, "FailedToListEndpointSlices",
			"Error listing Endpoint Slices for Service %s/%s: %v", service.Namespace, service.Name, err)
		return err
	}

	// 丢弃已标记删除的 EndpointSlice，避免控制器被这些即将消失的对象卡住。
	endpointSlices = dropEndpointSlicesPendingDeletion(endpointSlices)

	if c.endpointSliceTracker.StaleSlices(service, endpointSlices) {
		return endpointslicepkg.NewStaleInformerCache("EndpointSlice informer cache is out of date")
	}

	// 此处调用 ComputeEndpointLastChangeTriggerTime，即使本次同步是空操作、不更新 EndpointSlice，
	// 也要确保触发时间追踪器的状态被刷新，避免后续变更的时间戳统计失真。
	lastChangeTriggerTime := c.triggerTimeTracker.
		ComputeEndpointLastChangeTriggerTime(namespace, service, pods)

	err = c.reconciler.Reconcile(logger, service, pods, endpointSlices, lastChangeTriggerTime)
	if err != nil {
		c.eventRecorder.Eventf(service, v1.EventTypeWarning, "FailedToUpdateEndpointSlices",
			"Error updating Endpoint Slices for Service %s/%s: %v", service.Namespace, service.Name, err)
		return err
	}

	return nil
}

// podQueueWorker 持续从 podQueue 中取出并处理 Pod 投影键。
func (c *Controller) podQueueWorker(logger klog.Logger) {
	for c.processNextPodWorkItem(logger) {
	}
}

// processNextPodWorkItem 取出下一个 Pod 投影键，执行同步，并按错误情况安排重试或丢弃。
func (c *Controller) processNextPodWorkItem(logger klog.Logger) bool {
	cKey, quit := c.podQueue.Get()
	if quit {
		return false
	}
	defer c.podQueue.Done(cKey)

	err := c.syncPod(logger, cKey)
	c.handlePodErr(logger, err, cKey)

	return true
}

// handlePodErr 按重试预算处理 Pod 同步错误：成功则清除重试计数，
// 未超过上限则限速重新入队，超过上限则丢弃并上报错误。
func (c *Controller) handlePodErr(logger klog.Logger, err error, key *endpointsliceutil.PodProjectionKey) {
	if err == nil {
		c.podQueue.Forget(key)
		return
	}

	if c.podQueue.NumRequeues(key) < maxRetries {
		logger.V(2).Info("Error syncing pod, retrying", "PodProjectionKey", *key)
		c.podQueue.AddRateLimited(key)
		return
	}

	logger.Info("Dropping pod out of the queue", "PodProjectionKey", *key)
	c.podQueue.Forget(key)
	utilruntime.HandleError(err)
}

// syncPod 处理单个 Pod 投影键：找出所有可能受该 Pod 变化影响的 Service，
// 并将它们延迟一定时间后放入 serviceQueue，以合并同一 namespace 内大量 Service 的筛选开销。
func (c *Controller) syncPod(logger klog.Logger, key *endpointsliceutil.PodProjectionKey) error {
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing pod", "PodProjectionKey", *key, "elapsedTime", time.Since(startTime))
	}()

	servicesToUpdate, err := endpointsliceutil.GetServicesToUpdate(c.serviceLister, key)
	if err != nil {
		return err
	}

	for service := range servicesToUpdate {
		c.serviceQueue.AddAfter(service, c.endpointUpdatesBatchPeriod)
	}

	return nil
}

// onServiceUpdate 将 Service 入队处理。Selectors 的缓存由后续同步流程处理。
func (c *Controller) onServiceUpdate(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	c.serviceQueue.Add(key)
}

// onServiceDelete 从缓存中移除 Service Selector，并将该 Service 入队处理。
func (c *Controller) onServiceDelete(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	c.serviceQueue.Add(key)
}

// onPodUpdate 在 Pod 的 Add/Update/Delete 事件中将 Pod 的投影键入队，
// 以便在后续阶段查找所有匹配该 Pod 的 Service。
func (c *Controller) onPodUpdate(old, cur interface{}) {
	key := endpointsliceutil.GetPodUpdateProjectionKey(old, cur)
	if key != nil {
		c.podQueue.Add(key)
	}
}

// onEndpointSliceAdd 在 EndpointSlice 的资源版本与 endpointSliceTracker 中预期的不一致时，
// 将相关 Service 入队同步。
func (c *Controller) onEndpointSliceAdd(obj interface{}) {
	endpointSlice := obj.(*discovery.EndpointSlice)
	if endpointSlice == nil {
		utilruntime.HandleError(fmt.Errorf("Invalid EndpointSlice provided to onEndpointSliceAdd()"))
		return
	}
	if c.reconciler.ManagedByController(endpointSlice) && c.endpointSliceTracker.ShouldSync(endpointSlice) {
		c.queueServiceForEndpointSlice(endpointSlice)
	}
}

// onEndpointSliceUpdate 在 EndpointSlice 的资源版本与 tracker 中预期的不一致时，
// 或者其 managed-by 值变为/不再是本控制器时，将相关 Service 入队同步。
func (c *Controller) onEndpointSliceUpdate(logger klog.Logger, prevObj, obj interface{}) {
	prevEndpointSlice := prevObj.(*discovery.EndpointSlice)
	endpointSlice := obj.(*discovery.EndpointSlice)
	if endpointSlice == nil || prevEndpointSlice == nil {
		utilruntime.HandleError(fmt.Errorf("Invalid EndpointSlice provided to onEndpointSliceUpdate()"))
		return
	}
	// EndpointSlice 的 generation 不会因标签变化而改变。虽然本控制器不会修改 LabelServiceName，
	// 但用户可能会；此检查确保正确处理该标签的变更。
	svcName := endpointSlice.Labels[discovery.LabelServiceName]
	prevSvcName := prevEndpointSlice.Labels[discovery.LabelServiceName]
	if svcName != prevSvcName {
		logger.Info("label changed", "label", discovery.LabelServiceName, "oldService", prevSvcName, "newService", svcName, "endpointslice", klog.KObj(endpointSlice))
		c.queueServiceForEndpointSlice(endpointSlice)
		c.queueServiceForEndpointSlice(prevEndpointSlice)
		return
	}
	if c.reconciler.ManagedByChanged(prevEndpointSlice, endpointSlice) || (c.reconciler.ManagedByController(endpointSlice) && c.endpointSliceTracker.ShouldSync(endpointSlice)) {
		c.queueServiceForEndpointSlice(endpointSlice)
	}
}

// onEndpointSliceDelete 在 EndpointSlice 的资源版本与 tracker 中预期的不一致时，
// 将相关 Service 入队同步。
func (c *Controller) onEndpointSliceDelete(obj interface{}) {
	endpointSlice := getEndpointSliceFromDeleteAction(obj)
	if endpointSlice != nil && c.reconciler.ManagedByController(endpointSlice) && c.endpointSliceTracker.Has(endpointSlice) {
		// HandleDeletion 返回 false 表示这不是预期中的删除；
		// 此时需要再次入队 Service，以确保其状态被重新协调。
		if !c.endpointSliceTracker.HandleDeletion(endpointSlice) {
			c.queueServiceForEndpointSlice(endpointSlice)
		}
	}
}

// queueServiceForEndpointSlice 将给定 EndpointSlice 所属的 Service 入队同步。
// 入队带有延迟，用于合并多个 EndpointSlice 的连续变更，避免频繁触发同步。
func (c *Controller) queueServiceForEndpointSlice(endpointSlice *discovery.EndpointSlice) {
	key, err := endpointslicerec.ServiceControllerKey(endpointSlice)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for EndpointSlice %+v: %v", endpointSlice, err))
		return
	}

	// 取两个延迟中的较大值：既保证最小的冷却时间，又遵循配置的批量周期。
	delay := endpointSliceChangeMinSyncDelay
	if c.endpointUpdatesBatchPeriod > delay {
		delay = c.endpointUpdatesBatchPeriod
	}
	c.serviceQueue.AddAfter(key, delay)
}

// addNode 在节点新增时将固定的拓扑项入队，触发拓扑缓存刷新与分布检查。
func (c *Controller) addNode() {
	c.topologyQueue.Add(topologyQueueItemKey)
}

// updateNode 在节点的 Ready 状态或拓扑区域标签变化时将拓扑项入队。
// 云厂商可能在节点创建后异步添加 topology.kubernetes.io/zone 标签，
// 因此仅当这些与分布相关的字段发生变化时才需要重新计算。
func (c *Controller) updateNode(old, cur interface{}) {
	oldNode := old.(*v1.Node)
	curNode := cur.(*v1.Node)

	// LabelTopologyZone may be added by cloud provider asynchronously after the Node is created.
	// The topology cache should be updated in this case.
	if isNodeReady(oldNode) != isNodeReady(curNode) ||
		oldNode.Labels[v1.LabelTopologyZone] != curNode.Labels[v1.LabelTopologyZone] {
		c.topologyQueue.Add(topologyQueueItemKey)
	}
}

// deleteNode 在节点删除时将拓扑项入队，以便重新计算节点分布并更新相关提示。
func (c *Controller) deleteNode() {
	c.topologyQueue.Add(topologyQueueItemKey)
}

// checkNodeTopologyDistribution 用最新的节点列表刷新拓扑缓存，
// 并将因节点变化而超出拓扑分布阈值的 Service 入队重新协调。
func (c *Controller) checkNodeTopologyDistribution(logger klog.Logger) {
	if c.topologyCache == nil {
		return
	}
	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Error listing Nodes")
		return
	}
	c.topologyCache.SetNodes(logger, nodes)
	serviceKeys := c.topologyCache.GetOverloadedServices()
	for _, serviceKey := range serviceKeys {
		logger.V(2).Info("Queuing Service after Node change due to overloading", "key", serviceKey)
		c.serviceQueue.Add(serviceKey)
	}
}

// trackSync 按同步结果（success / stale / error）给 EndpointSliceSyncs 指标计数。
// 将缓存过期单独归类，便于观察 informer 缓存不一致对同步的影响。
func trackSync(err error) {
	metricLabel := "success"
	if err != nil {
		if endpointslicepkg.IsStaleInformerCacheErr(err) {
			metricLabel = "stale"
		} else {
			metricLabel = "error"
		}
	}
	endpointslicemetrics.EndpointSliceSyncs.WithLabelValues(metricLabel).Inc()
}

// dropEndpointSlicesPendingDeletion 过滤掉带有删除时间戳（正在删除）的 EndpointSlice。
// 在原切片上原地压缩以避免额外分配；跳过这些对象可防止控制器反复处理即将消失的资源而卡住。
func dropEndpointSlicesPendingDeletion(endpointSlices []*discovery.EndpointSlice) []*discovery.EndpointSlice {
	n := 0
	for _, endpointSlice := range endpointSlices {
		if endpointSlice.DeletionTimestamp == nil {
			endpointSlices[n] = endpointSlice
			n++
		}
	}
	return endpointSlices[:n]
}

// getEndpointSliceFromDeleteAction 从删除事件的对象中提取 EndpointSlice。
// 事件对象既可能是 EndpointSlice 本身，也可能是 cache.DeletedFinalStateUnknown 墓碑；
// 后者用于 informer 未能观察到最终删除状态时，需要从墓碑中取回原始对象。
func getEndpointSliceFromDeleteAction(obj interface{}) *discovery.EndpointSlice {
	if endpointSlice, ok := obj.(*discovery.EndpointSlice); ok {
		// 对刚加入的对象，需要把其所属的所有 Service 重新入队以重算端点；
		// 这里对删除场景也执行同样的处理，以确保 Service 的状态被刷新。
		return endpointSlice
	}
	// 到达这里说明删除事件携带的是墓碑对象，而非 EndpointSlice 本身，需要进一步解包。
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("Couldn't get object from tombstone %#v", obj))
		return nil
	}
	endpointSlice, ok := tombstone.Obj.(*discovery.EndpointSlice)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("Tombstone contained object that is not a EndpointSlice: %#v", obj))
		return nil
	}
	return endpointSlice
}

// isNodeReady 返回节点是否就绪；未就绪的节点不应参与端点分布计算。
// isNodeReady 判断节点是否处于 Ready 状态（Status.Conditions 中 NodeReady 为 True）。
// 拓扑分布计算依赖此状态，以排除尚未就绪的节点。
func isNodeReady(node *v1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}
