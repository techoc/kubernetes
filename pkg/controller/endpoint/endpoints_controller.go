/*
Copyright 2014 The Kubernetes Authors.

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

package endpoint

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/endpointslice"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/endpoints"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/controller"
	utillabels "k8s.io/kubernetes/pkg/util/labels"
	utilnet "k8s.io/utils/net"
)

const (
	// maxRetries 是 Service 被丢弃出队列前的重试次数。
	// 按当前速率限制器（5ms*2^(maxRetries-1)）， successive 重试间隔为：
	// 5ms、10ms、20ms、40ms、80ms、160ms、320ms、640ms、1.3s、2.6s、5.1s、10.2s、20.4s、41s、82s。
	maxRetries = 15

	// maxCapacity 是单个 Endpoints 资源中允许存储的地址数量上限。
	// 未来版本可能会截断超过此长度的 endpoint。
	maxCapacity = 1000

	// truncated 是 Endpoints 上 `endpoints.kubernetes.io/over-capacity` 注解的取值，
	// 表示 endpoint 数量已被截断到 maxCapacity。
	truncated = "truncated"

	// LabelManagedBy 用于识别由本控制器管理的 Endpoints 的标签。
	LabelManagedBy = "endpoints.kubernetes.io/managed-by"

	// ControllerName 是本控制器的名称。
	ControllerName = "endpoint-controller"
)

// NewEndpointController 构造一个 EndpointController，并注册 Service、Pod、Endpoints 的事件处理。
// Service 与 Endpoints 事件直接进入 service 队列；Pod 事件先进入 podQueue，
// 由 podWorker 反查受影响的 Service，避免每个 Pod 事件都遍历命名空间下所有 Service。
func NewEndpointController(ctx context.Context, podInformer coreinformers.PodInformer, serviceInformer coreinformers.ServiceInformer,
	endpointsInformer coreinformers.EndpointsInformer, client clientset.Interface, endpointUpdatesBatchPeriod time.Duration) *Controller {
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: ControllerName})

	e := &Controller{
		client: client,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "endpoint",
			},
		),
		podQueue:         workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*endpointsliceutil.PodProjectionKey]()),
		workerLoopPeriod: time.Second,
	}

	// Service 的增删改都要重新计算其 Endpoints；更新时不比较 old 与 cur，因为标签和选择器都可能需要同步。
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: e.onServiceUpdate,
		UpdateFunc: func(old, cur interface{}) {
			e.onServiceUpdate(cur)
		},
		DeleteFunc: e.onServiceDelete,
	})
	e.serviceLister = serviceInformer.Lister()
	e.servicesSynced = serviceInformer.Informer().HasSynced

	// Pod 事件只需入队 Pod 的投影键；是否为 Endpoints 贡献地址由 syncService 依据当前状态判断。
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { e.onPodUpdate(nil, obj) },
		UpdateFunc: e.onPodUpdate,
		DeleteFunc: func(obj interface{}) { e.onPodUpdate(obj, nil) },
	})
	e.podLister = podInformer.Lister()
	e.podsSynced = podInformer.Informer().HasSynced

	// Endpoints 被外部删除时也要重新入队对应 Service，以便控制器重建该对象。
	endpointsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: e.onEndpointsDelete,
	})
	e.endpointsLister = endpointsInformer.Lister()
	e.endpointsSynced = endpointsInformer.Informer().HasSynced

	// 陈旧跟踪器记录已处理过的 resourceVersion，防止 informer 缓存回退导致基于旧快照的错误更新。
	e.staleEndpointsTracker = newStaleEndpointsTracker()
	e.triggerTimeTracker = endpointsliceutil.NewTriggerTimeTracker()
	e.eventBroadcaster = broadcaster
	e.eventRecorder = recorder

	e.endpointUpdatesBatchPeriod = endpointUpdatesBatchPeriod

	return e
}

// Controller 基于 Service 的标签选择器维护对应的 Endpoints 资源。
type Controller struct {
	client           clientset.Interface
	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	// serviceLister 用于查询 Service，数据来自传给 NewEndpointController 的共享 informer。
	serviceLister corelisters.ServiceLister
	// servicesSynced 表示 Service 共享 informer 至少完成过一次同步。
	// 作为结构成员是为了在测试中注入。
	servicesSynced cache.InformerSynced

	// podLister 用于查询 Pod，数据来自传给 NewEndpointController 的共享 informer。
	podLister corelisters.PodLister
	// podsSynced 表示 Pod 共享 informer 至少完成过一次同步。
	// 作为结构成员是为了在测试中注入。
	podsSynced cache.InformerSynced

	// endpointsLister 用于查询 Endpoints，数据来自传给 NewEndpointController 的共享 informer。
	endpointsLister corelisters.EndpointsLister
	// endpointsSynced 表示 Endpoints 共享 informer 至少完成过一次同步。
	// 作为结构成员是为了在测试中注入。
	endpointsSynced cache.InformerSynced
	// staleEndpointsTracker 用于判断缓存中的 Endpoints 是否已过期。
	staleEndpointsTracker *staleEndpointsTracker

	// queue 存放待更新的 Service。此处不能用 channel：
	// channel 会让 Pod 多的 Service 被处理得更频繁，且同一 Service 多次入队会被重复处理；
	// workqueue 则会对相同 key 去重。
	queue workqueue.TypedRateLimitingInterface[string]

	// podQueue 用于计算 Pod 到 Service 的映射，并把匹配到的 Service 推入 service 队列。
	// 命名空间内 Service 很多、需要逐个求值标签选择器时开销较大，故与 service 队列分离。
	podQueue workqueue.TypedRateLimitingInterface[*endpointsliceutil.PodProjectionKey]

	// workerLoopPeriod 是 worker 两次运行之间的间隔，负责处理 Service 与 Pod 变更队列。
	workerLoopPeriod time.Duration

	// triggerTimeTracker 用于计算并导出 EndpointsLastChangeTriggerTime 注解，
	// 记录触发 Endpoints 变更的最早事件时间。
	triggerTimeTracker *endpointsliceutil.TriggerTimeTracker

	// endpointUpdatesBatchPeriod 用于延迟 Pod 触发的 Service 更新，以便将短时间内的多个 Pod 变更合并成一次 Endpoints 更新。
	endpointUpdatesBatchPeriod time.Duration
}

// Run 启动控制器，直到 ctx 被取消才返回。
// workers 决定并行处理 Service 与 Pod 队列的 worker 数量。
func (e *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件广播管线，将 Event 写入日志及 apiserver。
	e.eventBroadcaster.StartStructuredLogging(3)
	e.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: e.client.CoreV1().Events("")})
	defer e.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting endpoint controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down endpoint controller")
		e.queue.ShutDown()
		e.podQueue.ShutDown()
		wg.Wait()
	}()

	// 必须等三类 informer 都同步后再启动 worker，否则首次全量同步可能被误判为大量变更。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, e.podsSynced, e.servicesSynced, e.endpointsSynced) {
		return
	}

	// 每对 worker 分别消费 service 队列与 pod 队列；另起一个 goroutine 做启动时的孤儿 Endpoints 清理。
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, e.worker, e.workerLoopPeriod)
		})
		wg.Go(func() {
			wait.UntilWithContext(ctx, e.podWorker, e.workerLoopPeriod)
		})
	}
	wg.Go(func() {
		e.checkLeftoverEndpoints()
	})
	// 阻塞等待 ctx 结束；defer 中负责关闭队列并等待所有 worker 退出。
	<-ctx.Done()
}

// podToEndpointAddressForService 为 Service 选择一个与 Service IP 家族匹配的 Pod IP，
// 构造 EndpointAddress（含 NodeName 与指向 Pod 的 TargetRef）。
func podToEndpointAddressForService(svc *v1.Service, pod *v1.Pod) (*v1.EndpointAddress, error) {
	var endpointIP string
	ipFamily := v1.IPv4Protocol

	// IPFamilies 通常由 apiserver 默认值填充，但通过 watch 事件到达的 Service 可能为空。
	// 此时从 ClusterIP 或 Pod IP 推断地址家族，避免对 IPFamilies[0] 越界访问。
	if len(svc.Spec.IPFamilies) > 0 {
		ipFamily = svc.Spec.IPFamilies[0]
	} else if len(svc.Spec.ClusterIP) > 0 && svc.Spec.ClusterIP != v1.ClusterIPNone {
		if utilnet.IsIPv6String(svc.Spec.ClusterIP) {
			ipFamily = v1.IPv6Protocol
		}
	} else if utilnet.IsIPv6String(pod.Status.PodIP) {
		ipFamily = v1.IPv6Protocol
	}
	wantIPv6 := ipFamily == v1.IPv6Protocol

	// 选择与目标家族匹配的 IP。这里对 IP 解析后再 String 化，
	// 以便把 Pod 上非规范格式的 IP 归一化。
	for _, podIP := range pod.Status.PodIPs {
		ip := utilnet.ParseIPSloppy(podIP.IP)
		if wantIPv6 == utilnet.IsIPv6(ip) {
			endpointIP = ip.String()
			break
		}
	}

	if endpointIP == "" {
		return nil, fmt.Errorf("failed to find a matching endpoint for service %v", svc.Name)
	}

	return &v1.EndpointAddress{
		IP:       endpointIP,
		NodeName: &pod.Spec.NodeName,
		TargetRef: &v1.ObjectReference{
			Kind:      "Pod",
			Namespace: pod.ObjectMeta.Namespace,
			Name:      pod.ObjectMeta.Name,
			UID:       pod.ObjectMeta.UID,
		},
	}, nil
}

// onServiceUpdate 将发生变更的 Service 入队等待处理。
func (e *Controller) onServiceUpdate(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		// 取 key 失败说明对象不符合预期，记录错误后跳过；不能把无 key 的对象放入队列。
		return
	}
	e.queue.Add(key)
}

// onServiceDelete 将已删除的 Service 入队，以便清理其对应的 Endpoints。
func (e *Controller) onServiceDelete(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}
	e.queue.Add(key)
}

// onPodUpdate 在 Pod 增删或更新时把其投影键放入 podQueue，
// 由 podWorker 决定哪些 Service 需要更新 Endpoints。
func (e *Controller) onPodUpdate(old, cur interface{}) {
	key := endpointsliceutil.GetPodUpdateProjectionKey(old, cur)
	if key != nil {
		e.podQueue.Add(key)
	}
}

// onEndpointsDelete 在 Endpoints 被外部删除时把同名 Service 入队，
// 使控制器有机会重建它。
func (e *Controller) onEndpointsDelete(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}
	e.queue.Add(key)
}

// worker 循环从队列取出并处理 Service，直到队列关闭。
// 可并行运行多个 worker：workqueue 保证同一 Service 不会被并发处理。
func (e *Controller) worker(ctx context.Context) {
	for e.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理 service 队列中的单个条目，
// 无论成功与否都调用 Done，确保条目能被后续重试。
func (e *Controller) processNextWorkItem(ctx context.Context) bool {
	eKey, quit := e.queue.Get()
	if quit {
		return false
	}
	// Done 必须与 Get 成对出现，否则条目会一直处于处理中状态而无法再次入队。
	defer e.queue.Done(eKey)

	logger := klog.FromContext(ctx)
	err := e.syncService(ctx, eKey)
	e.handleErr(logger, err, eKey)

	return true
}

// handleErr 统一处理同步错误：成功则 Forget；失败且未超过 maxRetries 则限速重试；
// 超过上限则丢弃并上报错误，防止错误条目无限占用队列。
func (e *Controller) handleErr(logger klog.Logger, err error, key string) {
	if err == nil {
		e.queue.Forget(key)
		return
	}

	ns, name, keyErr := cache.SplitMetaNamespaceKey(key)
	if keyErr != nil {
		logger.Error(err, "Failed to split meta namespace cache key", "key", key)
	}

	if e.queue.NumRequeues(key) < maxRetries {
		logger.V(2).Info("Error syncing endpoints, retrying", "service", klog.KRef(ns, name), "err", err)
		e.queue.AddRateLimited(key)
		return
	}

	logger.Info("Dropping service out of the queue", "service", klog.KRef(ns, name), "err", err)
	e.queue.Forget(key)
	utilruntime.HandleError(err)
}

// syncService 是核心协调逻辑：根据 Service 选择器从缓存中收集 Pod，
// 构造 Endpoints 子集并与现状比较，仅在必要时创建或更新 Endpoints 对象。
func (e *Controller) syncService(ctx context.Context, key string) error {
	startTime := time.Now()
	logger := klog.FromContext(ctx)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	// 用 defer 记录同步耗时，便于观测控制器延迟。
	defer func() {
		logger.V(4).Info("Finished syncing service endpoints", "service", klog.KRef(namespace, name), "startTime", time.Since(startTime))
	}()

	service, err := e.serviceLister.Services(namespace).Get(name)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		// Service 已删除，删除其对应的 Endpoints。
		// TODO: 注意，如果控制器恰好在 Service 删除时宕机，就会漏掉该删除事件，
		// 因此这里并未彻底解决问题，详见 #6877。
		err = e.client.CoreV1().Endpoints(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		e.triggerTimeTracker.DeleteService(namespace, name)
		e.staleEndpointsTracker.Delete(namespace, name)
		return nil
	}

	// ExternalName 类型的 Service 不由本控制器维护 Endpoints，也无需创建对应对象。
	if service.Spec.Type == v1.ServiceTypeExternalName {
		// ExternalName 类型的 Service 不接收本控制器生成的 Endpoints；
		// 参考 https://issues.k8s.io/105986
		return nil
	}

	// 无选择器的 Service 不由本控制器生成 Endpoints；
	// 这类 Service 的 Endpoints 由用户通过 REST API 在带外创建。
	if service.Spec.Selector == nil {
		// services without a selector receive no endpoints from this controller;
		return nil
	}

	logger.V(5).Info("About to update endpoints for service", "service", klog.KRef(namespace, name))
	// 从本地缓存按选择器筛出需要出现在 Endpoints 中的 Pod。
	pods, err := e.podLister.Pods(service.Namespace).List(labels.Set(service.Spec.Selector).AsSelectorPreValidated())
	if err != nil {
		// 从本地缓存读取，正常路径下不应出错。
		return err
	}

	// 即使本次同步最终是 no-op，也要更新 triggerTimeTracker 的状态，
	// 保证后续有实际变更时能计算出正确的触发时间。
	endpointsLastChangeTriggerTime := e.triggerTimeTracker.
		ComputeEndpointLastChangeTriggerTime(namespace, service, pods)

	subsets := []v1.EndpointSubset{}
	var totalReadyEps int
	var totalNotReadyEps int

	for _, pod := range pods {
		// 过滤掉不应出现在 Endpoints 中的 Pod（如已终止或 IP 未分配）。
		if !endpointsliceutil.ShouldPodBeInEndpoints(pod, service.Spec.PublishNotReadyAddresses) {
			logger.V(5).Info("Pod is not included on endpoints for Service", "pod", klog.KObj(pod), "service", klog.KObj(service))
			continue
		}

		ep, err := podToEndpointAddressForService(service, pod)
		if err != nil {
			// 混用双栈与非双栈节点的集群（如升级期间）可能找不到匹配家族的 IP；
			// 跳过该 Pod 而不是报错，避免阻塞整个 Service 的同步。
			logger.V(2).Info("Failed to find endpoint for service with ClusterIP on pod with error", "service", klog.KObj(service), "clusterIP", service.Spec.ClusterIP, "pod", klog.KObj(pod), "error", err)
			continue
		}

		// 按 Service 要求决定是否把 Pod 的 hostname 写入 EndpointAddress。
		epa := *ep
		if endpointsliceutil.ShouldSetHostname(pod, service) {
			epa.Hostname = pod.Spec.Hostname
		}

		// headless Service 可以没有端口。
		if len(service.Spec.Ports) == 0 {
			if service.Spec.ClusterIP == api.ClusterIPNone {
				subsets, totalReadyEps, totalNotReadyEps = addEndpointSubset(logger, subsets, pod, epa, nil, service.Spec.PublishNotReadyAddresses)
				// 无端口的 headless Service 无需重新打包子集。
			}
		} else {
			// 每个 ServicePort 都要在 Pod 容器上找到对应端口后才能生成 EndpointPort。
			for i := range service.Spec.Ports {
				servicePort := &service.Spec.Ports[i]
				portNum, err := endpointslice.FindPort(pod, servicePort)
				if err != nil {
					logger.V(4).Info("Failed to find port for service", "service", klog.KObj(service), "error", err)
					continue
				}
				epp := endpointPortFromServicePort(servicePort, portNum)

				var readyEps, notReadyEps int
				subsets, readyEps, notReadyEps = addEndpointSubset(logger, subsets, pod, epa, epp, service.Spec.PublishNotReadyAddresses)
				totalReadyEps = totalReadyEps + readyEps
				totalNotReadyEps = totalNotReadyEps + notReadyEps
			}
		}
	}
	// RepackSubsets 合并可归并的子集，减少 Endpoints 对象体积。
	subsets = endpoints.RepackSubsets(subsets)

	// 检查计算出的 Endpoints 与现状是否真的存在差异。
	currentEndpoints, err := e.endpointsLister.Endpoints(service.Namespace).Get(service.Name)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		currentEndpoints = &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name:   service.Name,
				Labels: service.Labels,
			},
		}
		// 若缓存中的 Endpoints 版本已被本控制器处理过，说明 informer 缓存落后于 API，
		// 直接返回错误触发重试，避免用旧数据覆盖更新的对象。
	} else if e.staleEndpointsTracker.IsStale(currentEndpoints) {
		return fmt.Errorf("endpoints informer cache is out of date, resource version %s already processed for endpoints %s", currentEndpoints.ResourceVersion, key)
	}

	// ResourceVersion 为空说明缓存中没有该 Endpoints，需要新建。
	createEndpoints := len(currentEndpoints.ResourceVersion) == 0

	// 比较排序后的子集和标签。比较子集时忽略 Pod 的 ResourceVersion，
	// 避免修改注解之类与本控制器无关的 Pod 更新触发无意义的 Endpoints 更新。
	if !createEndpoints &&
		// 若首次比较失败，重新打包后再试一次：不同哈希算法可能导致子集划分不同（见 issue #129652）。
		(endpointSubsetsEqualIgnoreResourceVersion(currentEndpoints.Subsets, subsets) ||
			// 若首次比较失败，可能是哈希算法差异导致子集划分不同，
			// 详见 https://github.com/kubernetes/kubernetes/issues/129652#issuecomment-3264035333
			endpointSubsetsEqualIgnoreResourceVersion(endpoints.RepackSubsets(currentEndpoints.Subsets), subsets)) &&
		labelsCorrectForEndpoints(currentEndpoints.Labels, service.Labels) &&
		capacityAnnotationSetCorrectly(currentEndpoints.Annotations, currentEndpoints.Subsets) {
		logger.V(5).Info("endpoints are equal, skipping update", "service", klog.KObj(service))
		return nil
	}
	newEndpoints := currentEndpoints.DeepCopy()
	newEndpoints.Subsets = subsets
	newEndpoints.Labels = service.Labels
	if newEndpoints.Annotations == nil {
		newEndpoints.Annotations = make(map[string]string)
	}

	// 有新的触发时间则写入注解；否则清除旧注解，避免残留过期信息。
	if !endpointsLastChangeTriggerTime.IsZero() {
		newEndpoints.Annotations[v1.EndpointsLastChangeTriggerTime] =
			endpointsLastChangeTriggerTime.UTC().Format(time.RFC3339Nano)
		// 超过容量上限时截断 endpoint 并打上 over-capacity 注解；未超限时清除该注解。
	} else { // No new trigger time, clear the annotation.
		delete(newEndpoints.Annotations, v1.EndpointsLastChangeTriggerTime)
	}

	if truncateEndpoints(newEndpoints) {
		newEndpoints.Annotations[v1.EndpointsOverCapacity] = truncated
	} else {
		delete(newEndpoints.Annotations, v1.EndpointsOverCapacity)
	}

	if newEndpoints.Labels == nil {
		newEndpoints.Labels = make(map[string]string)
	}

	// 用 IsHeadlessService 标签标识无 ClusterIP 的 Service，便于客户端路由。
	if !helper.IsServiceIPSet(service) {
		newEndpoints.Labels = utillabels.CloneAndAddLabel(newEndpoints.Labels, v1.IsHeadlessService, "")
	} else {
		newEndpoints.Labels = utillabels.CloneAndRemoveLabel(newEndpoints.Labels, v1.IsHeadlessService)
	}
	// 打上 managed-by 标签，声明这些 Endpoints 归本控制器所有。
	newEndpoints.Labels[LabelManagedBy] = ControllerName

	logger.V(4).Info("Update endpoints", "service", klog.KObj(service), "readyEndpoints", totalReadyEps, "notreadyEndpoints", totalNotReadyEps)
	var updatedEndpoints *v1.Endpoints
	// 根据是否已有 Endpoints 决定走创建还是更新路径。
	if createEndpoints {
		// No previous endpoints, create them
		_, err = e.client.CoreV1().Endpoints(service.Namespace).Create(ctx, newEndpoints, metav1.CreateOptions{})
	} else {
		// Pre-existing
		updatedEndpoints, err = e.client.CoreV1().Endpoints(service.Namespace).Update(ctx, newEndpoints, metav1.UpdateOptions{})
	}
	// 创建失败多数是因为命名空间正在终止或策略配置错误。
	if err != nil {
		if createEndpoints && errors.IsForbidden(err) {
			// Forbidden 主要有两种原因：
			// 1. 命名空间处于 Terminating 状态，默认不允许创建 Endpoints；
			// 2. 策略配置错误，此时任何 Service 都无法正常工作。
			// 第一种情况很常见，因此用较低日志级别记录。
			logger.V(5).Info("Forbidden from creating endpoints", "error", err)

			// 命名空间正在终止时创建会一直失败，直接丢弃该条目不再重试。
			if errors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
				return nil
			}
		}

		// 创建或更新失败时发出 Warning 事件，便于用户通过 kubectl describe 排查。
		if createEndpoints {
			e.eventRecorder.Eventf(newEndpoints, v1.EventTypeWarning, "FailedToCreateEndpoint", "Failed to create endpoint for service %v/%v: %v", service.Namespace, service.Name, err)
		} else {
			e.eventRecorder.Eventf(newEndpoints, v1.EventTypeWarning, "FailedToUpdateEndpoint", "Failed to update endpoint %v/%v: %v", service.Namespace, service.Name, err)
		}

		return err
	}
	// 记录已被覆盖的旧版本号：若之后又从 lister 读到相同 resourceVersion，
	// 说明缓存尚未追上 API（如 webhook 或截断使更新成为 no-op 并返回相同版本），需要稍后重试。
	// 详见 https://issues.k8s.io/127370 与 https://issues.k8s.io/126578
	if updatedEndpoints != nil && updatedEndpoints.ResourceVersion != currentEndpoints.ResourceVersion {
		e.staleEndpointsTracker.Stale(currentEndpoints)
	}
	return nil
}

// podWorker 循环消费 podQueue。Pod 事件本身不直接更新 Endpoints，
// 而是先找出标签选择器匹配该 Pod 的 Service，再把这些 Service 推入 service 队列。
func (e *Controller) podWorker(ctx context.Context) {
	for e.processNextPodWorkItem(ctx) {
	}
}

// processNextPodWorkItem 处理 podQueue 中的单个条目，并保证 Done 与 Get 配对。
func (e *Controller) processNextPodWorkItem(ctx context.Context) bool {
	eKey, quit := e.podQueue.Get()
	if quit {
		return false
	}
	defer e.podQueue.Done(eKey)

	logger := klog.FromContext(ctx)
	err := e.syncPod(logger, eKey)
	e.handlePodErr(logger, err, eKey)

	return true
}

// handlePodErr 与 handleErr 策略一致：成功 Forget，失败限速重试，超过上限则丢弃。
// 避免反复失败的 Pod 投影键长期滞留在队列中。
func (e *Controller) handlePodErr(logger klog.Logger, err error, key *endpointsliceutil.PodProjectionKey) {
	if err == nil {
		e.podQueue.Forget(key)
		return
	}

	if e.podQueue.NumRequeues(key) < maxRetries {
		logger.V(2).Info("Error syncing pod, retrying", "PodProjectionKey", *key)
		e.podQueue.AddRateLimited(key)
		return
	}

	logger.Info("Dropping pod out of the queue", "PodProjectionKey", *key)
	e.podQueue.Forget(key)
	utilruntime.HandleError(err)
}

// syncPod 根据 Pod 的命名空间与标签变更，求出需要重新计算 Endpoints 的 Service 集合。
// 使用 AddAfter 按 batchPeriod 延迟入队，将短时间内大量 Pod 变更合并，减轻 API 压力。
func (e *Controller) syncPod(logger klog.Logger, key *endpointsliceutil.PodProjectionKey) error {
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing pod", "PodProjectionKey", *key, "elapsedTime", time.Since(startTime))
	}()

	servicesToUpdate, err := endpointsliceutil.GetServicesToUpdate(e.serviceLister, key)
	if err != nil {
		return err
	}

	// 延迟入队使同一批 Pod 变更只触发一次 Service 同步。
	for service := range servicesToUpdate {
		e.queue.AddAfter(service, e.endpointUpdatesBatchPeriod)
	}

	return nil
}

// checkLeftoverEndpoints 在启动时列出所有 Endpoints 并将其对应的 Service 入队，
// 以清理没有对应 Service 的孤儿 Endpoints。只需执行一次：稳态下的孤儿会被正常检测到，
// 仅控制器重启时可能遗留漏网之鱼。
func (e *Controller) checkLeftoverEndpoints() {
	list, err := e.endpointsLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Unable to list endpoints (%v); orphaned endpoints will not be cleaned up. (They're pretty harmless, but you can restart this component if you want another attempt made.)", err))
		return
	}
	for _, ep := range list {
		// 多副本 controller-manager 场景下，leader election 记录也存为无对应 Service 的 Endpoints；
		// 若误删会在约 5 分钟后触发重新选举，故必须跳过。
		if _, ok := ep.Annotations[resourcelock.LeaderElectionRecordAnnotationKey]; ok {
			// 多副本 controller-manager 场景下，会观察到 leader-election Endpoints 在 5 分钟后被删除，
			// 从而触发重新选举；由于 leader-election 只有 Endpoints 而没有 Service，这里跳过删除。
			continue
		}
		key, err := controller.KeyFunc(ep)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("Unable to get key for endpoint %#v", ep))
			continue
		}
		e.queue.Add(key)
	}
}

// addEndpointSubset 把一个 EndpointAddress 及其端口加入 Ready 或 NotReady 子集：
// Pod 就绪或 Service 设置了 PublishNotReadyAddresses 时进入 Addresses，否则进入 NotReadyAddresses。
// 调用方须先用 ShouldPodBeInEndpoints 过滤 Pod；返回两个计数分别统计 ready 与 not-ready 地址数。
func addEndpointSubset(logger klog.Logger, subsets []v1.EndpointSubset, pod *v1.Pod, epa v1.EndpointAddress,
	epp *v1.EndpointPort, tolerateUnreadyEndpoints bool) ([]v1.EndpointSubset, int, int) {
	var readyEps int
	var notReadyEps int
	ports := []v1.EndpointPort{}
	if epp != nil {
		ports = append(ports, *epp)
	}
	// 根据 Pod 状态与 Service 的容忍策略决定地址归入哪个字段。
	if tolerateUnreadyEndpoints || podutil.IsPodReady(pod) {
		subsets = append(subsets, v1.EndpointSubset{
			Addresses: []v1.EndpointAddress{epa},
			Ports:     ports,
		})
		readyEps++
		// Pod 未就绪且 Service 不容忍时，地址只能放入 NotReadyAddresses。
	} else { // if it is not a ready address it has to be not ready
		logger.V(5).Info("Pod is out of service", "pod", klog.KObj(pod))
		subsets = append(subsets, v1.EndpointSubset{
			NotReadyAddresses: []v1.EndpointAddress{epa},
			Ports:             ports,
		})
		notReadyEps++
	}
	return subsets, readyEps, notReadyEps
}

// endpointPortFromServicePort 将 ServicePort 转换为 Endpoints 中使用的 EndpointPort。
func endpointPortFromServicePort(servicePort *v1.ServicePort, portNum int) *v1.EndpointPort {
	return &v1.EndpointPort{
		Name:        servicePort.Name,
		Port:        int32(portNum),
		Protocol:    servicePort.Protocol,
		AppProtocol: servicePort.AppProtocol,
	}
}

// capacityAnnotationSetCorrectly 校验容量注解与子集状态是否一致：
// 地址数超过 maxCapacity 时必须已截断（注解存在），否则不应带该注解。
func capacityAnnotationSetCorrectly(annotations map[string]string, subsets []v1.EndpointSubset) bool {
	numEndpoints := 0
	for _, subset := range subsets {
		numEndpoints += len(subset.Addresses) + len(subset.NotReadyAddresses)
	}
	// 截断前先统计 Ready 与 NotReady 地址总数。
	if numEndpoints > maxCapacity {
		// If subsets are over capacity, they must be truncated so consider
		// the annotation as not set correctly
		return false
	}
	_, ok := annotations[v1.EndpointsOverCapacity]
	return !ok
}

// truncateEndpoints 按各子集 endpoint 数量的比例尽力截断，
// 并优先保留 Ready 地址、丢弃 NotReady 地址。
func truncateEndpoints(endpoints *v1.Endpoints) bool {
	totalReady := 0
	totalNotReady := 0
	for _, subset := range endpoints.Subsets {
		totalReady += len(subset.Addresses)
		totalNotReady += len(subset.NotReadyAddresses)
	}

	// 未超过上限则无需截断。
	if totalReady+totalNotReady <= maxCapacity {
		return false
	}

	// 优先保留 Ready 地址：仅当 Ready 数量本身超限才截断 Ready；
	// 否则先用剩余容量容纳 NotReady 地址。
	truncateReady := false
	max := maxCapacity - totalReady
	numTotal := totalNotReady
	if totalReady > maxCapacity {
		truncateReady = true
		max = maxCapacity
		numTotal = totalReady
	}
	canBeAdded := max

	// 按各子集在总数中的占比分配可保留的 endpoint 名额。
	for i := range endpoints.Subsets {
		subset := endpoints.Subsets[i]
		numInSubset := len(subset.Addresses)
		if !truncateReady {
			numInSubset = len(subset.NotReadyAddresses)
		}

		// 每个子集可分到的数量按其在总数中的占比向上取整，
		// 因此最后一个子集实际保留的数量通常会略小于理论比例。
		toBeAdded := int(math.Ceil((float64(numInSubset) / float64(numTotal)) * float64(max)))
		// 最后一个子集可能分到超过剩余容量的名额，需收敛到剩余容量，
		// 避免写入超出 maxCapacity 的地址。
		if toBeAdded > canBeAdded {
			toBeAdded = canBeAdded
		}

		// Ready 超限时按比例截断 Ready 地址，并清空所有 NotReady 地址。
		if truncateReady {
			// 按比例截断 Ready 地址，并丢弃全部 NotReady 地址。
			subset.Addresses = addressSubset(subset.Addresses, toBeAdded)
			subset.NotReadyAddresses = []v1.EndpointAddress{}
			canBeAdded -= len(subset.Addresses)
			// 只需截断 NotReady 地址。
		} else {
			// 只截断 NotReady 地址。
			subset.NotReadyAddresses = addressSubset(subset.NotReadyAddresses, toBeAdded)
			canBeAdded -= len(subset.NotReadyAddresses)
		}
		endpoints.Subsets[i] = subset
	}
	return true
}

// addressSubset 返回地址列表的前 maxNum 个；若数量不超过 maxNum 则原样返回。
func addressSubset(addresses []v1.EndpointAddress, maxNum int) []v1.EndpointAddress {
	if len(addresses) <= maxNum {
		return addresses
	}
	return addresses[0:maxNum]
}

// semanticIgnoreResourceVersion 做深度相等比较，但忽略 ObjectReference 的 ResourceVersion。
// 比较 Endpoints/EndpointSlice 中的 Pod 引用时用它排除 Pod 版本变化带来的无意义更新。
var semanticIgnoreResourceVersion = conversion.EqualitiesOrDie(
	// 比较前清空两个对象的 ResourceVersion 字段，使版本差异不影响相等性判断。
	func(a, b v1.ObjectReference) bool {
		a.ResourceVersion = ""
		b.ResourceVersion = ""
		return a == b
	},
)

// endpointSubsetsEqualIgnoreResourceVersion 判断两个 EndpointSubset 列表语义相等，
// 但忽略其中 Pod 引用的 ResourceVersion。
func endpointSubsetsEqualIgnoreResourceVersion(subsets1, subsets2 []v1.EndpointSubset) bool {
	return semanticIgnoreResourceVersion.DeepEqual(subsets1, subsets2)
}

// labelsCorrectForEndpoints 校验 epLabels 是否由 svcLabels 正确派生
// （忽略 v1.IsHeadlessService 标签）。
func labelsCorrectForEndpoints(epLabels, svcLabels map[string]string) bool {
	// managed-by 标签必须指向本控制器，否则 Endpoints 可能被其他控制器接管。
	if epLabels[LabelManagedBy] != ControllerName {
		return false
	}

	// 除 IsHeadlessService 与 LabelManagedBy 外，epLabels 的每个键值都必须与 svcLabels 一致，
	// 且数量也要对应，确保两侧标签集合完全同步。
	skipped := 0
	for k, v := range epLabels {
		if k == v1.IsHeadlessService || k == LabelManagedBy {
			skipped++
		} else if sv, exists := svcLabels[k]; !exists || sv != v {
			return false
		}
		// 数量相等意味着两边标签一一对应，没有多余或缺失的标签。
	}
	return len(svcLabels) == len(epLabels)-skipped
}
