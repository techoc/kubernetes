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

package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/api"
	servicehelper "k8s.io/cloud-provider/service/helpers"
	"k8s.io/component-base/featuregate"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	"k8s.io/klog/v2"
)

const (
	// Interval of synchronizing service status from apiserver
	// 定期全量重同步 Service，兜住 informer 事件丢失或云侧状态漂移。
	serviceSyncPeriod = 30 * time.Second
	// Interval of synchronizing node status from apiserver
	// 节点变化会影响所有 LB 的后端，重同步周期比 Service 更长以降低云 API 压力。
	nodeSyncPeriod = 100 * time.Second

	// How long to wait before retrying the processing of a service change.
	// If this changes, the sleep in hack/jenkins/e2e.sh before downing a cluster
	// should be changed appropriately.
	// 失败重试采用指数退避，范围 5s~300s；e2e 脚本中的等待时间与此假设相关。
	minRetryDelay = 5 * time.Second
	maxRetryDelay = 300 * time.Second
	// ToBeDeletedTaint is a taint used by the CLuster Autoscaler before marking a node for deletion. Defined in
	// https://github.com/kubernetes/autoscaler/blob/e80ab518340f88f364fe3ef063f8303755125971/cluster-autoscaler/utils/deletetaint/delete.go#L36
	ToBeDeletedTaint = "ToBeDeletedByClusterAutoscaler"
)

// cachedService 保存 Service 的最近一次已知状态。
// 仅用于非 finalizer 删除路径下的云资源清理，不能当作 informer 缓存使用。
type cachedService struct {
	// The cached state of the service
	state *v1.Service
}

// serviceCache 是 key -> Service 状态的并发安全映射。
// 读写来自 service worker 与 node worker，因此所有访问都必须持锁。
type serviceCache struct {
	mu         sync.RWMutex // protects serviceMap
	serviceMap map[string]*cachedService
}

// Controller keeps cloud provider service resources
// (like load balancers) in sync with the registry.
// Controller 就 Service 期望状态与云负载均衡实际状态做收敛：
// Service 变更由 serviceQueue 驱动，节点变更由 nodeQueue 驱动并扇出到所有 LB。
type Controller struct {
	cloud       cloudprovider.Interface
	kubeClient  clientset.Interface
	clusterName string
	balancer    cloudprovider.LoadBalancer
	// TODO(#85155): Stop relying on this and remove the cache completely.
	// cache 记录 Service 状态，用于无 finalizer 的删除路径；finalizer 路径不依赖它。
	cache               *serviceCache
	serviceLister       corelisters.ServiceLister // 只读访问 informer 缓存，不能直接修改返回对象。
	serviceListerSynced cache.InformerSynced
	eventBroadcaster    record.EventBroadcaster
	eventRecorder       record.EventRecorder
	nodeLister          corelisters.NodeLister // 提供 LB 后端节点集合；过滤谓词决定哪些节点可用。
	nodeListerSynced    cache.InformerSynced
	// services and nodes that need to be synced
	serviceQueue workqueue.TypedRateLimitingInterface[string]
	nodeQueue    workqueue.TypedRateLimitingInterface[string]
	// lastSyncedNodes is used when reconciling node state and keeps track of
	// the last synced set of nodes per service key. This is accessed from the
	// service and node controllers, hence it is protected by a lock.
	// lastSyncedNodes 记录每个 Service 上次同步给云厂商的节点集合，用于跳过
	// 无实质变化的更新。service worker 与 node worker 都会访问，故需加锁。
	lastSyncedNodes     map[string][]*v1.Node
	lastSyncedNodesLock sync.Mutex
}

// New returns a new service controller to keep cloud provider service resources
// (like load balancers) in sync with the registry.
// New 只负责接线与注册事件处理器，实际循环在 Run 中启动。
func New(
	cloud cloudprovider.Interface,
	kubeClient clientset.Interface,
	serviceInformer coreinformers.ServiceInformer,
	nodeInformer coreinformers.NodeInformer,
	clusterName string,
	featureGate featuregate.FeatureGate,
) (*Controller, error) {
	// 指标注册放在最前面，保证即使后续初始化失败，指标也已可用。
	registerMetrics()

	s := &Controller{
		cloud:            cloud,
		kubeClient:       kubeClient,
		clusterName:      clusterName,
		cache:            &serviceCache{serviceMap: make(map[string]*cachedService)},
		nodeLister:       nodeInformer.Lister(),
		nodeListerSynced: nodeInformer.Informer().HasSynced,
		serviceQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](minRetryDelay, maxRetryDelay),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "service"},
		),
		nodeQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](minRetryDelay, maxRetryDelay),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "node"},
		),
		lastSyncedNodes: make(map[string][]*v1.Node),
	}

	// init 校验云提供商是否实现了 LoadBalancer 接口；不支持时直接拒绝创建控制器。
	if err := s.init(); err != nil {
		return nil, err
	}

	// Service 事件驱动单 Service 的增删改；resync 周期保证最终一致性。
	serviceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(cur interface{}) {
				svc, ok := cur.(*v1.Service)
				// Check cleanup here can provide a remedy when controller failed to handle
				// changes before it exiting (e.g. crashing, restart, etc.).
				// 重启后重放 Add 事件时，带清理需求的对象也会被重新入队，弥补崩溃前的遗漏。
				if ok && (wantsLoadBalancer(svc) || needsCleanup(svc)) {
					s.enqueueService(cur)
				}
			},
			UpdateFunc: func(old, cur interface{}) {
				// 只有影响 LB 的字段变化或需要清理时才入队，降低无效同步。
				oldSvc, ok1 := old.(*v1.Service)
				curSvc, ok2 := cur.(*v1.Service)
				if ok1 && ok2 && (needsUpdate(oldSvc, curSvc) || needsCleanup(curSvc)) {
					s.enqueueService(cur)
				}
			},
			// No need to handle deletion event because the deletion would be handled by
			// the update path when the deletion timestamp is added.
			// Service 的最终删除由 update（deletionTimestamp 被设置）触发清理，不监听 Delete。
		},
		serviceSyncPeriod,
	)
	s.serviceLister = serviceInformer.Lister()
	s.serviceListerSynced = serviceInformer.Informer().HasSynced

	// 节点事件驱动所有 LB 的后端重算；新增节点必须通知，更新则先做谓词过滤。
	nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(cur interface{}) {
				s.enqueueNode(cur)
			},
			UpdateFunc: func(old, cur interface{}) {
				oldNode, ok := old.(*v1.Node)
				if !ok {
					return
				}

				curNode, ok := cur.(*v1.Node)
				if !ok {
					return
				}

				// 无关变化（如其他 taint、非 Ready 条件）不应触发全量 LB 更新。
				if !shouldSyncUpdatedNode(oldNode, curNode) {
					return
				}

				s.enqueueNode(curNode)
			},
			DeleteFunc: func(old interface{}) {
				// 节点删除同样需要把其从 LB 后端移除。
				s.enqueueNode(old)
			},
		},
		nodeSyncPeriod,
	)

	return s, nil
}

// obj could be an *v1.Service, or a DeletionFinalStateUnknown marker item.
// 使用 DeletionHandlingMetaNamespaceKeyFunc，确保删除事件也能得到 key。
func (c *Controller) enqueueService(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}
	c.serviceQueue.Add(key)
}

// obj could be an *v1.Service, or a DeletionFinalStateUnknown marker item.
// 节点 key 只用于触发重算，不用于定位单个节点。
func (c *Controller) enqueueNode(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}
	c.nodeQueue.Add(key)
}

// Run starts a background goroutine that watches for changes to services that
// have (or had) LoadBalancers=true and ensures that they have
// load balancers created and deleted appropriately.
// serviceSyncPeriod controls how often we check the cluster's services to
// ensure that the correct load balancers exist.
// nodeSyncPeriod controls how often we check the cluster's nodes to determine
// if load balancers need to be updated to point to a new set.
//
// It's an error to call Run() more than once for a given ServiceController
// object.
// Run 启动事件广播、缓存同步与 worker 循环；同一实例重复调用会导致重复消费队列。
func (c *Controller) Run(ctx context.Context, workers int, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	// 事件广播器绑定运行期上下文，ctx 取消时自动停止分发。
	c.eventBroadcaster = record.NewBroadcaster(record.WithContext(ctx))
	c.eventRecorder = c.eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "service-controller"})

	// 保证 panic 不会带走进程，并让队列在退出时停止接收新 key。
	defer runtime.HandleCrash()
	defer c.serviceQueue.ShutDown()
	defer c.nodeQueue.ShutDown()

	// Start event processing pipeline.
	// 事件同时写入结构日志与 API Server，便于排障和用户可见。
	c.eventBroadcaster.StartStructuredLogging(0)
	c.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.kubeClient.CoreV1().Events("")})
	defer c.eventBroadcaster.Shutdown()

	klog.Info("Starting service controller")
	defer klog.Info("Shutting down service controller")
	controllerManagerMetrics.ControllerStarted("service")
	defer controllerManagerMetrics.ControllerStopped("service")

	// 必须等 Service 与 Node 缓存同步完成后再开始调谐，否则会把空集合当成真实状态。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.serviceListerSynced, c.nodeListerSynced) {
		return
	}

	// Service 同步允许并发，不同 key 之间相互独立。
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.serviceWorker, time.Second)
	}

	// Initialize one go-routine servicing node events. This ensure we only
	// process one node at any given moment in time
	// 节点同步只用一个 goroutine，避免多个 node sync 同时改写 LB 后端。
	go wait.UntilWithContext(ctx, func(ctx context.Context) { c.nodeWorker(ctx, workers) }, time.Second)

	<-ctx.Done()
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
// serviceWorker 循环消费 Service key；同名 key 由队列保证不会并发处理。
func (c *Controller) serviceWorker(ctx context.Context) {
	for c.processNextServiceItem(ctx) {
	}
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
// nodeWorker 消费节点 key，并调用 syncNodes 扇出到所有 LB。
func (c *Controller) nodeWorker(ctx context.Context, workers int) {
	for c.processNextNodeItem(ctx, workers) {
	}
}

// processNextNodeItem 处理一个节点变更：一次 syncNodes 覆盖所有 Service，
// 失败的服务 key 被投递到 serviceQueue 以复用 Service 的重试策略。
func (c *Controller) processNextNodeItem(ctx context.Context, workers int) bool {
	key, quit := c.nodeQueue.Get()
	if quit {
		return false
	}
	defer c.nodeQueue.Done(key)

	for serviceToRetry := range c.syncNodes(ctx, workers) {
		c.serviceQueue.Add(serviceToRetry)
	}

	c.nodeQueue.Forget(key)
	return true
}

// processNextServiceItem 处理一个 Service key，并根据错误类型选择重试方式。
func (c *Controller) processNextServiceItem(ctx context.Context) bool {
	key, quit := c.serviceQueue.Get()
	if quit {
		return false
	}
	defer c.serviceQueue.Done(key)

	err := c.syncService(ctx, key)
	if err == nil {
		// 成功后清空该 key 的失败计数，避免长期保留退避状态。
		c.serviceQueue.Forget(key)
		return true
	}

	// RetryError 携带云厂商建议的精确重试时间（如 LB 正在创建中），优先于指数退避。
	var re *api.RetryError
	if errors.As(err, &re) {
		klog.Warningf("error processing service %v (retrying in %s): %v", key, re.RetryAfter(), err)
		c.serviceQueue.AddAfter(key, re.RetryAfter())
	} else {
		runtime.HandleError(fmt.Errorf("error processing service %v (retrying with exponential backoff): %v", key, err))
		c.serviceQueue.AddRateLimited(key)
	}

	return true
}

// init 校验并缓存 LoadBalancer 实现；没有云提供商时仍允许运行其他控制器。
func (c *Controller) init() error {
	if c.cloud == nil {
		return fmt.Errorf("WARNING: no cloud provider provided, services of type LoadBalancer will fail")
	}

	// LoadBalancer() 的 ok=false 表示该云不支持外部 LB，Service 控制器无法工作。
	balancer, ok := c.cloud.LoadBalancer()
	if !ok {
		return fmt.Errorf("the cloud provider does not support external load balancers")
	}
	c.balancer = balancer

	return nil
}

// processServiceCreateOrUpdate operates loadbalancers for the incoming service accordingly.
// Returns an error if processing the service update failed.
// 该函数维护 cache 与最终状态：只有当 LB 删除成功后才移除缓存。
func (c *Controller) processServiceCreateOrUpdate(ctx context.Context, service *v1.Service, key string) error {
	// TODO(@MrHohn): Remove the cache once we get rid of the non-finalizer deletion
	// path. Ref https://github.com/kubernetes/enhancements/issues/980.
	cachedService := c.cache.getOrCreate(key)
	if cachedService.state != nil && cachedService.state.UID != service.UID {
		// This happens only when a service is deleted and re-created
		// in a short period, which is only possible when it doesn't
		// contain finalizer.
		// UID 变化意味着同名 Service 已被重建，必须先用旧对象清理旧的云资源。
		if err := c.processLoadBalancerDelete(ctx, cachedService.state, key); err != nil {
			return err
		}
	}
	// Always cache the service, we need the info for service deletion in case
	// when load balancer cleanup is not handled via finalizer.
	// 即便后续同步失败也要缓存，删除事件才能拿到足够信息做兜底清理。
	cachedService.state = service
	op, err := c.syncLoadBalancerIfNeeded(ctx, service, key)
	if err != nil {
		c.eventRecorder.Eventf(service, v1.EventTypeWarning, "SyncLoadBalancerFailed", "Error syncing load balancer: %v", err)
		return err
	}
	if op == deleteLoadBalancer {
		// Only delete the cache upon successful load balancer deletion.
		c.cache.delete(key)
	}

	return nil
}

// loadBalancerOperation 表示一次调谐对 LB 采取的动作。
type loadBalancerOperation int

const (
	deleteLoadBalancer loadBalancerOperation = iota
	ensureLoadBalancer
	// 日志中最多列出 20 个节点名，避免大集群日志爆炸。
	maxNodeNamesToLog = 20
)

// syncLoadBalancerIfNeeded ensures that service's status is synced up with loadbalancer
// i.e. creates loadbalancer for service if requested and deletes loadbalancer if the service
// doesn't want a loadbalancer no more. Returns whatever error occurred.
// finalizer 的增删与云资源操作严格配对：加 finalizer 后再创建，删除成功后再移除。
func (c *Controller) syncLoadBalancerIfNeeded(ctx context.Context, service *v1.Service, key string) (loadBalancerOperation, error) {
	// Note: It is safe to just call EnsureLoadBalancer.  But, on some clouds that requires a delete & create,
	// which may involve service interruption.  Also, we would like user-friendly events.

	// Save the state so we can avoid a write if it doesn't change
	// 保存旧状态用于比较，避免无变化的 status patch 触发不必要的 watch 事件。
	previousStatus := service.Status.LoadBalancer.DeepCopy()
	var newStatus *v1.LoadBalancerStatus
	var op loadBalancerOperation
	var err error

	// 删除分支：Service 不再需要 LB，或带清理 finalizer 且即将/已经删除。
	if !wantsLoadBalancer(service) || needsCleanup(service) {
		// Delete the load balancer if service no longer wants one, or if service needs cleanup.
		op = deleteLoadBalancer
		newStatus = &v1.LoadBalancerStatus{}
		// 先查询是否存在，避免对不存在的 LB 发起无意义的删除调用。
		_, exists, err := c.balancer.GetLoadBalancer(ctx, c.clusterName, service)
		if err != nil {
			return op, fmt.Errorf("failed to check if load balancer exists before cleanup: %v", err)
		}
		if exists {
			klog.V(2).Infof("Deleting existing load balancer for service %s", key)
			c.eventRecorder.Event(service, v1.EventTypeNormal, "DeletingLoadBalancer", "Deleting load balancer")
			if err := c.balancer.EnsureLoadBalancerDeleted(ctx, c.clusterName, service); err != nil {
				// ImplementedElsewhere 表示该 Service 由其他控制器负责，删除视为成功。
				if err == cloudprovider.ImplementedElsewhere {
					klog.V(4).Infof("LoadBalancer for service %s implemented by a different controller %s, Ignoring error on deletion", key, c.cloud.ProviderName())
				} else {
					return op, fmt.Errorf("failed to delete load balancer: %v", err)
				}
			}
		}
		// Always remove finalizer when load balancer is deleted, this ensures Services
		// can be deleted after all corresponding load balancer resources are deleted.
		// 只有云资源清理成功后才移除 finalizer，防止 Service 先被删除导致资源泄漏。
		if err := c.removeFinalizer(service); err != nil {
			return op, fmt.Errorf("failed to remove load balancer cleanup finalizer: %v", err)
		}
		c.eventRecorder.Event(service, v1.EventTypeNormal, "DeletedLoadBalancer", "Deleted load balancer")
	} else {
		// Create or update the load balancer if service wants one.
		// 先加 finalizer 再创建，保证创建过程中 Service 不会被删除。
		op = ensureLoadBalancer
		klog.V(2).Infof("Ensuring load balancer for service %s", key)
		c.eventRecorder.Event(service, v1.EventTypeNormal, "EnsuringLoadBalancer", "Ensuring load balancer")
		// Always add a finalizer prior to creating load balancers, this ensures Services
		// can't be deleted until all corresponding load balancer resources are also deleted.
		if err := c.addFinalizer(service); err != nil {
			return op, fmt.Errorf("failed to add load balancer cleanup finalizer: %v", err)
		}
		newStatus, err = c.ensureLoadBalancer(ctx, service)
		if err != nil {
			if err == cloudprovider.ImplementedElsewhere {
				// ImplementedElsewhere indicates that the ensureLoadBalancer is a nop and the
				// 不属于本控制器的 Service，不写 status、不改事件，直接放弃。
				// functionality is implemented by a different controller.  In this case, we
				// return immediately without doing anything.
				klog.V(4).Infof("LoadBalancer for service %s implemented by a different controller %s, Ignoring error", key, c.cloud.ProviderName())
				return op, nil
			}
			// Use %w deliberately so that a returned RetryError can be handled.
			// 保留错误链，processNextServiceItem 才能识别 RetryError。
			return op, fmt.Errorf("failed to ensure load balancer: %w", err)
		}
		if newStatus == nil {
			return op, fmt.Errorf("service status returned by EnsureLoadBalancer is nil")
		}

		c.eventRecorder.Event(service, v1.EventTypeNormal, "EnsuredLoadBalancer", "Ensured load balancer")
	}

	if err := c.patchStatus(service, previousStatus, newStatus); err != nil {
		// Only retry error that isn't not found:
		// NotFound 通常意味着 finalizer 移除后 Service 立即消失，重试没有意义。
		// - Not found error mostly happens when service disappears right after
		//   we remove the finalizer.
		// - We can't patch status on non-exist service anyway.
		if !apierrors.IsNotFound(err) {
			return op, fmt.Errorf("failed to update load balancer status: %v", err)
		}
	}

	return op, nil
}

// ensureLoadBalancer 计算稳定的后端节点集合并调用云接口创建/更新 LB。
func (c *Controller) ensureLoadBalancer(ctx context.Context, service *v1.Service) (*v1.LoadBalancerStatus, error) {
	// 使用 stableNodeSetPredicates（而非 Ready 谓词），避免节点短暂 NotReady
	// 就把它从 LB 摘除，从而触发连接中断。
	nodes, err := listWithPredicates(c.nodeLister, stableNodeSetPredicates...)
	if err != nil {
		return nil, err
	}
	// If there are no available nodes for LoadBalancer service, make a EventTypeWarning event for it.
	// 空后端仍交给云厂商处理（部分实现会创建空 LB），但事件提示用户检查节点。
	if len(nodes) == 0 {
		c.eventRecorder.Event(service, v1.EventTypeWarning, "UnAvailableLoadBalancer", "There are no available nodes for LoadBalancer")
	}
	// 记录本次同步的节点集合，后续节点同步据此跳过无变化更新。
	c.storeLastSyncedNodes(service, nodes)
	// - Not all cloud providers support all protocols and the next step is expected to return
	//   an error for unsupported protocols
	status, err := c.balancer.EnsureLoadBalancer(ctx, c.clusterName, service, nodes)
	if err != nil {
		return nil, err
	}
	return status, nil
}

// storeLastSyncedNodes 以 namespace/name 为 key 记录最近一次同步的节点集合。
func (c *Controller) storeLastSyncedNodes(svc *v1.Service, nodes []*v1.Node) {
	c.lastSyncedNodesLock.Lock()
	defer c.lastSyncedNodesLock.Unlock()
	key, _ := cache.MetaNamespaceKeyFunc(svc)
	c.lastSyncedNodes[key] = nodes
}

// getLastSyncedNodes 返回上次同步的节点集合；未同步过时返回 nil。
func (c *Controller) getLastSyncedNodes(svc *v1.Service) []*v1.Node {
	c.lastSyncedNodesLock.Lock()
	defer c.lastSyncedNodesLock.Unlock()
	key, _ := cache.MetaNamespaceKeyFunc(svc)
	return c.lastSyncedNodes[key]
}

// ListKeys implements the interface required by DeltaFIFO to list the keys we
// already know about.
// 返回当前缓存中的所有 key；调用方只应读取，不应修改底层 map。
func (s *serviceCache) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.serviceMap))
	for k := range s.serviceMap {
		keys = append(keys, k)
	}
	return keys
}

// GetByKey returns the value stored in the serviceMap under the given key
// 实现 cache.Store 语义：不存在时返回 (nil,false,nil)。
func (s *serviceCache) GetByKey(key string) (interface{}, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.serviceMap[key]; ok {
		return v, true, nil
	}
	return nil, false, nil
}

// ListKeys implements the interface required by DeltaFIFO to list the keys we
// already know about.
// allServices 返回缓存中所有 Service 的快照，用于节点变化时的全量扇出。
// 注意：这里复制切片但不深拷贝 Service 对象，调用方不得修改返回值。
func (s *serviceCache) allServices() []*v1.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()
	services := make([]*v1.Service, 0, len(s.serviceMap))
	for _, v := range s.serviceMap {
		services = append(services, v.state)
	}
	return services
}

// get 读取缓存条目；返回值可能被并发修改，调用方需自行保证使用安全。
func (s *serviceCache) get(serviceName string) (*cachedService, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	service, ok := s.serviceMap[serviceName]
	return service, ok
}

// getOrCreate 原子地获取或创建缓存条目，避免先 get 再 set 的竞态。
func (s *serviceCache) getOrCreate(serviceName string) *cachedService {
	s.mu.Lock()
	defer s.mu.Unlock()
	service, ok := s.serviceMap[serviceName]
	if !ok {
		service = &cachedService{}
		s.serviceMap[serviceName] = service
	}
	return service
}

// set 覆盖写入缓存条目。
func (s *serviceCache) set(serviceName string, service *cachedService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serviceMap[serviceName] = service
}

// delete 移除缓存条目；重复删除是安全的。
func (s *serviceCache) delete(serviceName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.serviceMap, serviceName)
}

// needsCleanup checks if load balancer needs to be cleaned up as indicated by finalizer.
// 判定条件：带 LB finalizer，且（正在删除 或 已不再需要 LB）。
func needsCleanup(service *v1.Service) bool {
	if !servicehelper.HasLBFinalizer(service) {
		return false
	}

	if service.ObjectMeta.DeletionTimestamp != nil {
		return true
	}

	// Service doesn't want loadBalancer but owns loadBalancer finalizer also need to be cleaned up.
	// 类型被改回非 LoadBalancer 时也必须清理遗留的云资源。
	if service.Spec.Type != v1.ServiceTypeLoadBalancer {
		return true
	}

	return false
}

// needsUpdate checks if load balancer needs to be updated due to change in attributes.
// 逐项比较所有会影响 LB 的字段；任一变化都返回 true。无关字段变化不触发同步。
func needsUpdate(oldService *v1.Service, newService *v1.Service) bool {
	if !wantsLoadBalancer(oldService) && !wantsLoadBalancer(newService) {
		return false
	}
	if wantsLoadBalancer(oldService) != wantsLoadBalancer(newService) {
		klog.V(2).Infof("Service %s wants load balancer changed from %s to %s", klog.KObj(oldService), oldService.Spec.Type, newService.Spec.Type)
		return true
	}

	// 源地址范围直接决定 LB 防火墙规则，必须精确比较。
	if wantsLoadBalancer(newService) && !reflect.DeepEqual(oldService.Spec.LoadBalancerSourceRanges, newService.Spec.LoadBalancerSourceRanges) {
		klog.V(2).Infof("Service %s LoadBalancerSourceRanges changed from %v to %v", klog.KObj(newService), oldService.Spec.LoadBalancerSourceRanges, newService.Spec.LoadBalancerSourceRanges)
		return true
	}

	// 端口、会话保持及其配置都会改变 LB 转发行为。
	if !portsEqualForLB(oldService, newService) || oldService.Spec.SessionAffinity != newService.Spec.SessionAffinity {
		return true
	}

	if !reflect.DeepEqual(oldService.Spec.SessionAffinityConfig, newService.Spec.SessionAffinityConfig) {
		return true
	}
	if !loadBalancerIPsAreEqual(oldService, newService) {
		klog.V(2).Infof("Service %s LoadBalancerIP changed from %s to %s", klog.KObj(newService), oldService.Spec.LoadBalancerIP, newService.Spec.LoadBalancerIP)
		return true
	}
	if len(oldService.Spec.ExternalIPs) != len(newService.Spec.ExternalIPs) {
		klog.V(2).Infof("Service %s ExternalIPs' count changed from %v to %v", klog.KObj(newService), len(oldService.Spec.ExternalIPs), len(newService.Spec.ExternalIPs))
		return true
	}
	for i := range oldService.Spec.ExternalIPs {
		if oldService.Spec.ExternalIPs[i] != newService.Spec.ExternalIPs[i] {
			klog.V(2).Infof("Service %s ExternalIPs[%d] changed from %v to %v", klog.KObj(newService), i, oldService.Spec.ExternalIPs[i], newService.Spec.ExternalIPs[i])
			return true
		}
	}
	if !reflect.DeepEqual(oldService.Annotations, newService.Annotations) {
		return true
	}
	if oldService.UID != newService.UID {
		klog.V(2).Infof("Service %s UID changed from %s to %s", klog.KObj(newService), oldService.UID, newService.UID)
		return true
	}
	if oldService.Spec.ExternalTrafficPolicy != newService.Spec.ExternalTrafficPolicy {
		klog.V(2).Infof("Service %s ExternalTrafficPolicy changed from %s to %s", klog.KObj(newService), oldService.Spec.ExternalTrafficPolicy, newService.Spec.ExternalTrafficPolicy)
		return true
	}
	if oldService.Spec.HealthCheckNodePort != newService.Spec.HealthCheckNodePort {
		klog.V(2).Infof("Service %s HealthCheckNodePort changed from %v to %v", klog.KObj(newService), oldService.Spec.HealthCheckNodePort, newService.Spec.HealthCheckNodePort)
		return true
	}

	// User can upgrade (add another clusterIP or ipFamily) or can downgrade (remove secondary clusterIP or ipFamily),
	// but CAN NOT change primary/secondary clusterIP || ipFamily UNLESS they are changing from/to/ON ExternalName
	// so not care about order, only need check the length.
	// API 已禁止原地修改 IPFamily，这里只需按数量判断双栈升级/降级。
	if len(oldService.Spec.IPFamilies) != len(newService.Spec.IPFamilies) {
		klog.V(2).Infof("Service %s IPFamilies' count changed from %d to %d", klog.KObj(newService), len(oldService.Spec.IPFamilies), len(newService.Spec.IPFamilies))
		return true
	}

	return false
}

// getPortsForLB 返回 Service 端口的指针切片，仅用于比较，不复制元素。
func getPortsForLB(service *v1.Service) []*v1.ServicePort {
	ports := []*v1.ServicePort{}
	for i := range service.Spec.Ports {
		sp := &service.Spec.Ports[i]
		ports = append(ports, sp)
	}
	return ports
}

// portsEqualForLB 比较两个 Service 的端口列表是否等价（保持顺序）。
func portsEqualForLB(x, y *v1.Service) bool {
	xPorts := getPortsForLB(x)
	yPorts := getPortsForLB(y)
	return portSlicesEqualForLB(xPorts, yPorts)
}

// portSlicesEqualForLB 要求长度与顺序都一致；Service 端口顺序本身有意义。
func portSlicesEqualForLB(x, y []*v1.ServicePort) bool {
	if len(x) != len(y) {
		return false
	}

	for i := range x {
		if !portEqualForLB(x[i], y[i]) {
			return false
		}
	}
	return true
}

// portEqualForLB 比较单个端口的全部转发相关属性。
func portEqualForLB(x, y *v1.ServicePort) bool {
	// TODO: Should we check name?  (In theory, an LB could expose it)
	// 端口名目前已在下方的 Name 比较中覆盖。
	if x.Name != y.Name {
		return false
	}

	if x.Protocol != y.Protocol {
		return false
	}

	if x.Port != y.Port {
		return false
	}

	if x.NodePort != y.NodePort {
		return false
	}

	if x.TargetPort != y.TargetPort {
		return false
	}

	if !reflect.DeepEqual(x.AppProtocol, y.AppProtocol) {
		return false
	}

	return true
}

// nodeNames 提取节点名集合并去重，用于日志与集合比较。
func nodeNames(nodes []*v1.Node) sets.String {
	ret := sets.NewString()
	for _, node := range nodes {
		ret.Insert(node.Name)
	}
	return ret
}

// loggableNodeNames 截断超大节点集合，避免日志行过长。
func loggableNodeNames(nodes []*v1.Node) []string {
	if len(nodes) > maxNodeNamesToLog {
		skipped := len(nodes) - maxNodeNamesToLog
		names := nodeNames(nodes[:maxNodeNamesToLog]).List()
		return append(names, fmt.Sprintf("<%d more>", skipped))
	}
	return nodeNames(nodes).List()
}

// shouldSyncUpdatedNode 判断节点更新是否需要触发 LB 重算。
// 只看会影响后端集合的字段：是否被排除、providerID 是否变化。
func shouldSyncUpdatedNode(oldNode, newNode *v1.Node) bool {
	// Evaluate the individual node exclusion predicate before evaluating the
	// compounded result of all predicates.
	//
	// The sync function compares lastSyncedNodes with the new (existing) set of nodes
	// for each service, so services which are synced with the same set of nodes
	// should be skipped internally in the sync function. This is needed as to
	// trigger a global sync for all services and make sure no service gets
	// skipped due to a changing node predicate.
	// 只比较单个谓词而非全部谓词，避免某个节点从 Ready 变为 NotReady 时
	// 触发全量 LB 更新（stableNodeSetPredicates 不包含 Ready 条件）。
	if respectsPredicates(oldNode, nodeIncludedPredicate) != respectsPredicates(newNode, nodeIncludedPredicate) {
		return true
	}
	// For the same reason as above, also check for any change to the providerID
	// providerID 是云侧实例身份，变化会影响 LB 后端寻址。
	if oldNode.Spec.ProviderID != newNode.Spec.ProviderID {
		return true
	}

	return false
}

// syncNodes handles updating the hosts pointed to by all load
// balancers whenever the set of nodes in the cluster changes.
// 返回需要重试的 Service key 集合；调用方负责把它们重新入队。
func (c *Controller) syncNodes(ctx context.Context, workers int) sets.String {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Seconds()
		klog.V(4).Infof("It took %v seconds to finish syncNodes", latency)
		nodeSyncLatency.Observe(latency)
	}()

	klog.V(2).Infof("Syncing backends for all LB services.")
	// 只遍历缓存中的 Service（即曾经需要 LB 的对象），避免全量 List。
	servicesToUpdate := c.cache.allServices()
	numServices := len(servicesToUpdate)
	servicesToRetry := c.updateLoadBalancerHosts(ctx, servicesToUpdate, workers)
	klog.V(2).Infof("Successfully updated %d out of %d load balancers to direct traffic to the updated set of nodes",
		numServices-len(servicesToRetry), numServices)
	return servicesToRetry
}

// nodeSyncService syncs the nodes for one load balancer type service. The return value
// indicates if we should retry. Hence, this functions returns false if we've updated
// load balancers and finished doing it successfully, or didn't try to at all because
// there's no need. This function returns true if we tried to update load balancers and
// failed, indicating to the caller that we should try again.
// 这里只做 Read-Modify-Write 中的后两步，Service 对象来自缓存快照，不做 status 回写。
func (c *Controller) nodeSyncService(svc *v1.Service) bool {
	const retSuccess = false
	const retNeedRetry = true
	if svc == nil || !wantsLoadBalancer(svc) {
		return retSuccess
	}
	// 先取全量再做稳定节点过滤，便于与上次同步集合做对比。
	newNodes, err := listWithPredicates(c.nodeLister)
	if err != nil {
		runtime.HandleError(fmt.Errorf("failed to retrieve node list: %v", err))
		nodeSyncErrorCount.Inc()
		return retNeedRetry
	}
	newNodes = filterWithPredicates(newNodes, stableNodeSetPredicates...)
	oldNodes := filterWithPredicates(c.getLastSyncedNodes(svc), stableNodeSetPredicates...)
	// Store last synced nodes without actually determining if we successfully
	// synced them or not. Failed node syncs are passed off to retries in the
	// service queue, so no need to wait. If we don't store it now, we risk
	// re-syncing all LBs twice, one from another sync in the node sync and
	// from the service sync
	// 失败由 serviceQueue 重试，所以此处仍先记录新集合，避免重复扇出。
	c.storeLastSyncedNodes(svc, newNodes)
	// 节点集合无实质变化时直接跳过，避免频繁调用云 API。
	if nodesSufficientlyEqual(oldNodes, newNodes) {
		return retSuccess
	}
	klog.V(4).Infof("nodeSyncService started for service %s/%s", svc.Namespace, svc.Name)
	if err := c.lockedUpdateLoadBalancerHosts(svc, newNodes); err != nil {
		runtime.HandleError(fmt.Errorf("failed to update load balancer hosts for service %s/%s: %v", svc.Namespace, svc.Name, err))
		nodeSyncErrorCount.Inc()
		return retNeedRetry
	}
	klog.V(4).Infof("nodeSyncService finished successfully for service %s/%s", svc.Namespace, svc.Name)
	return retSuccess
}

// nodesSufficientlyEqual 判断两个节点集合在“影响 LB 的维度”上是否等价：
// 节点名集合与 providerID 均相同即可，忽略 Ready 等其他状态。
func nodesSufficientlyEqual(oldNodes, newNodes []*v1.Node) bool {
	if len(oldNodes) != len(newNodes) {
		return false
	}

	// This holds the Node fields which trigger a sync when changed.
	// 只提取必要字段，避免为比较而深拷贝整个 Node。
	type protoNode struct {
		providerID string
	}
	distill := func(n *v1.Node) protoNode {
		return protoNode{
			providerID: n.Spec.ProviderID,
		}
	}

	mOld := map[string]protoNode{}
	for _, n := range oldNodes {
		mOld[n.Name] = distill(n)
	}

	mNew := map[string]protoNode{}
	for _, n := range newNodes {
		mNew[n.Name] = distill(n)
	}

	return reflect.DeepEqual(mOld, mNew)
}

// updateLoadBalancerHosts updates all existing load balancers so that
// they will match the latest list of nodes with input number of workers.
// Returns the list of services that couldn't be updated.
// 并发度由 workers 控制；每个 Service 的云调用彼此独立。
func (c *Controller) updateLoadBalancerHosts(ctx context.Context, services []*v1.Service, workers int) (servicesToRetry sets.String) {
	klog.V(4).Infof("Running updateLoadBalancerHosts(len(services)==%d, workers==%d)", len(services), workers)

	// lock for servicesToRetry
	// ParallelizeUntil 的 worker 并发写入重试集合，必须加锁。
	servicesToRetry = sets.NewString()
	lock := sync.Mutex{}

	doWork := func(piece int) {
		if shouldRetry := c.nodeSyncService(services[piece]); !shouldRetry {
			return
		}
		lock.Lock()
		defer lock.Unlock()
		key := fmt.Sprintf("%s/%s", services[piece].Namespace, services[piece].Name)
		servicesToRetry.Insert(key)
	}
	workqueue.ParallelizeUntil(ctx, workers, len(services), doWork)
	klog.V(4).Infof("Finished updateLoadBalancerHosts")
	return servicesToRetry
}

// Updates the load balancer of a service, assuming we hold the mutex
// associated with the service.
// 函数名中的 locked 指调用方已持有该 Service 的并发控制权（当前由单 node worker 保证）。
func (c *Controller) lockedUpdateLoadBalancerHosts(service *v1.Service, hosts []*v1.Node) error {
	startTime := time.Now()
	loadBalancerSyncCount.Inc()
	defer func() {
		latency := time.Since(startTime).Seconds()
		klog.V(4).Infof("It took %v seconds to update load balancer hosts for service %s/%s", latency, service.Namespace, service.Name)
		updateLoadBalancerHostLatency.Observe(latency)
	}()
	klog.V(2).Infof("Updating backends for load balancer %s/%s with %d nodes: %v", service.Namespace, service.Name, len(hosts), loggableNodeNames(hosts))

	// This operation doesn't normally take very long (and happens pretty often), so we only record the final event
	// 该调用很频繁，只记录最终结果事件，避免事件刷屏。
	err := c.balancer.UpdateLoadBalancer(context.TODO(), c.clusterName, service, hosts)
	if err == nil {
		// If there are no available nodes for LoadBalancer service, make a EventTypeWarning event for it.
		if len(hosts) == 0 {
			c.eventRecorder.Event(service, v1.EventTypeWarning, "UnAvailableLoadBalancer", "There are no available nodes for LoadBalancer")
		} else {
			c.eventRecorder.Event(service, v1.EventTypeNormal, "UpdatedLoadBalancer", "Updated load balancer with new hosts")
		}
		return nil
	}
	if err == cloudprovider.ImplementedElsewhere {
		// ImplementedElsewhere indicates that the UpdateLoadBalancer is a nop and the
		// functionality is implemented by a different controller.  In this case, we
		// return immediately without doing anything.
		// 该 Service 不归本控制器管理，视为成功。
		return nil
	}
	// It's only an actual error if the load balancer still exists.
	// LB 已被删除时上游错误可忽略，删除流程会处理剩余清理。
	if _, exists, err := c.balancer.GetLoadBalancer(context.TODO(), c.clusterName, service); err != nil {
		runtime.HandleError(fmt.Errorf("failed to check if load balancer exists for service %s/%s: %v", service.Namespace, service.Name, err))
	} else if !exists {
		return nil
	}

	c.eventRecorder.Eventf(service, v1.EventTypeWarning, "UpdateLoadBalancerFailed", "Error updating load balancer with new hosts %v [node names limited, total number of nodes: %d], error: %v", loggableNodeNames(hosts), len(hosts), err)
	return err
}

// wantsLoadBalancer 判断 Service 是否由本控制器负责。
func wantsLoadBalancer(service *v1.Service) bool {
	// if LoadBalancerClass is set, the user does not want the default cloud-provider Load Balancer
	// LoadBalancerClass 非空表示交给自定义实现，本控制器不得介入。
	return service.Spec.Type == v1.ServiceTypeLoadBalancer && service.Spec.LoadBalancerClass == nil
}

// loadBalancerIPsAreEqual 比较用户指定的期望 LB IP；变化需要重新创建/更新 LB。
func loadBalancerIPsAreEqual(oldService, newService *v1.Service) bool {
	return oldService.Spec.LoadBalancerIP == newService.Spec.LoadBalancerIP
}

// syncService will sync the Service with the given key if it has had its expectations fulfilled,
// meaning it did not expect to see any more of its pods created or deleted. This function is not meant to be
// invoked concurrently with the same key.
// syncService 从 informer 读取最新对象并分派到创建/更新或删除路径。
func (c *Controller) syncService(ctx context.Context, key string) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing service %q (%v)", key, time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	// service holds the latest service info from apiserver
	// informer 缓存的对象是共享只读的，后续路径必须先 DeepCopy 再修改。
	service, err := c.serviceLister.Services(namespace).Get(name)
	switch {
	case apierrors.IsNotFound(err):
		// service absence in store means watcher caught the deletion, ensure LB info is cleaned
		// 缓存中不存在即视为已删除，走兜底清理路径（可能没有 finalizer）。
		err = c.processServiceDeletion(ctx, key)
	case err != nil:
		runtime.HandleError(fmt.Errorf("Unable to retrieve service %v from store: %v", key, err))
	default:
		// It is not safe to modify an object returned from an informer.
		// As reconcilers may modify the service object we need to copy
		// it first.
		// 深拷贝后，后续 patch/status 操作才不会污染所有控制器共享的缓存。
		err = c.processServiceCreateOrUpdate(ctx, service.DeepCopy(), key)
	}

	return err
}

// processServiceDeletion 在 Service 已从缓存消失后，用本地缓存做一次兜底清理。
func (c *Controller) processServiceDeletion(ctx context.Context, key string) error {
	cachedService, ok := c.cache.get(key)
	if !ok {
		// Cache does not contains the key means:
		// - We didn't create a Load Balancer for the deleted service at all.
		// - We already deleted the Load Balancer that was created for the service.
		// In both cases we have nothing left to do.
		return nil
	}
	klog.V(2).Infof("Service %v has been deleted. Attempting to cleanup load balancer resources", key)
	if err := c.processLoadBalancerDelete(ctx, cachedService.state, key); err != nil {
		return err
	}
	c.cache.delete(key)
	return nil
}

// processLoadBalancerDelete 调用云接口删除 LB，并发出成功/失败事件。
func (c *Controller) processLoadBalancerDelete(ctx context.Context, service *v1.Service, key string) error {
	// delete load balancer info only if the service type is LoadBalancer
	// 非 LB 类型没有对应云资源，直接跳过。
	if !wantsLoadBalancer(service) {
		return nil
	}
	c.eventRecorder.Event(service, v1.EventTypeNormal, "DeletingLoadBalancer", "Deleting load balancer")
	if err := c.balancer.EnsureLoadBalancerDeleted(ctx, c.clusterName, service); err != nil {
		c.eventRecorder.Eventf(service, v1.EventTypeWarning, "DeleteLoadBalancerFailed", "Error deleting load balancer: %v", err)
		return err
	}
	c.eventRecorder.Event(service, v1.EventTypeNormal, "DeletedLoadBalancer", "Deleted load balancer")
	return nil
}

// addFinalizer patches the service to add finalizer.
// 使用 patch 而非 update，减少与用户/其他控制器的写冲突。
func (c *Controller) addFinalizer(service *v1.Service) error {
	if servicehelper.HasLBFinalizer(service) {
		return nil
	}

	// Make a copy so we don't mutate the shared informer cache.
	updated := service.DeepCopy()
	updated.ObjectMeta.Finalizers = append(updated.ObjectMeta.Finalizers, servicehelper.LoadBalancerCleanupFinalizer)

	klog.V(2).Infof("Adding finalizer to service %s/%s", updated.Namespace, updated.Name)
	_, err := servicehelper.PatchService(c.kubeClient.CoreV1(), service, updated)
	return err
}

// removeFinalizer patches the service to remove finalizer.
// 移除后 Service 可被 API Server 真正删除，因此必须在云资源清理成功之后调用。
func (c *Controller) removeFinalizer(service *v1.Service) error {
	if !servicehelper.HasLBFinalizer(service) {
		return nil
	}

	// Make a copy so we don't mutate the shared informer cache.
	updated := service.DeepCopy()
	updated.ObjectMeta.Finalizers = removeString(updated.ObjectMeta.Finalizers, servicehelper.LoadBalancerCleanupFinalizer)

	klog.V(2).Infof("Removing finalizer from service %s/%s", updated.Namespace, updated.Name)
	_, err := servicehelper.PatchService(c.kubeClient.CoreV1(), service, updated)
	return err
}

// removeString returns a newly created []string that contains all items from slice that
// are not equal to s.
// 返回新切片，避免原地修改调用方（通常是 informer 对象）的数据。
func removeString(slice []string, s string) []string {
	var newSlice []string
	for _, item := range slice {
		if item != s {
			newSlice = append(newSlice, item)
		}
	}
	return newSlice
}

// patchStatus patches the service with the given LoadBalancerStatus.
// 状态未变化时直接返回，避免无意义的写放大。
func (c *Controller) patchStatus(service *v1.Service, previousStatus, newStatus *v1.LoadBalancerStatus) error {
	if servicehelper.LoadBalancerStatusEqual(previousStatus, newStatus) {
		return nil
	}
	// Make a copy so we don't mutate the shared informer cache.
	updated := service.DeepCopy()
	updated.Status.LoadBalancer = *newStatus

	klog.V(2).Infof("Patching status for service %s/%s", updated.Namespace, updated.Name)
	_, err := servicehelper.PatchService(c.kubeClient.CoreV1(), service, updated)
	return err
}

// NodeConditionPredicate is a function that indicates whether the given node's conditions meet
// some set of criteria defined by the function.
// 谓词只读访问 Node，必须是无副作用的纯函数，因为会被并发调用。
type NodeConditionPredicate func(node *v1.Node) bool

var (
	// allNodePredicates 用于需要完整可用性判断的路径（例如外部 ETP 的节点过滤）。
	allNodePredicates []NodeConditionPredicate = []NodeConditionPredicate{
		nodeIncludedPredicate,
		nodeUnTaintedPredicate,
		nodeReadyPredicate,
	}

	// stableNodeSetPredicates 故意不包含 Ready 谓词：节点短暂 NotReady 不应
	// 立刻从 LB 摘除，否则会中断现有连接并影响流量排空。
	stableNodeSetPredicates []NodeConditionPredicate = []NodeConditionPredicate{
		nodeNotDeletedPredicate,
		nodeIncludedPredicate,
		// This is not perfect, but probably good enough. We won't update the
		// LBs just because the taint was added (see shouldSyncUpdatedNode) but
		// if any other situation causes an LB sync, tainted nodes will be
		// excluded at that time and cause connections on said node to not
		// connection drain.
		nodeUnTaintedPredicate,
	}
)

// We consider the node for load balancing only when the node is not labelled for exclusion.
// 标签值按 bool 解析；无法解析时保守地排除该节点。
func nodeIncludedPredicate(node *v1.Node) bool {
	v, hasExcludeBalancerLabel := node.Labels[v1.LabelNodeExcludeBalancers]
	if hasExcludeBalancerLabel {
		v, err := strconv.ParseBool(v)
		if err != nil {
			return false
		}
		return !v
	}
	return !hasExcludeBalancerLabel
}

// We consider the node for load balancing only when its not tainted for deletion by the cluster autoscaler.
// 与 Cluster Autoscaler 的缩容协议对齐：带该 taint 的节点不再接收新流量。
func nodeUnTaintedPredicate(node *v1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == ToBeDeletedTaint {
			return false
		}
	}
	return true
}

// We consider the node for load balancing only when its NodeReady condition status is ConditionTrue
// 缺少 NodeReady 条件时视为不可用。
func nodeReadyPredicate(node *v1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady {
			return cond.Status == v1.ConditionTrue
		}
	}
	return false
}

// nodeNotDeletedPredicate 排除已标记删除的节点，避免把流量导向即将消失的后端。
func nodeNotDeletedPredicate(node *v1.Node) bool {
	return node.DeletionTimestamp.IsZero()
}

// listWithPredicate gets nodes that matches all predicate functions.
// 先全量 List 再本地过滤，避免为每种谓词组合构造 label selector。
func listWithPredicates(nodeLister corelisters.NodeLister, predicates ...NodeConditionPredicate) ([]*v1.Node, error) {
	nodes, err := nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	return filterWithPredicates(nodes, predicates...), nil
}

// filterWithPredicates 返回满足全部谓词的节点子集；输入顺序保持不变。
func filterWithPredicates(nodes []*v1.Node, predicates ...NodeConditionPredicate) []*v1.Node {
	var filtered []*v1.Node
	for i := range nodes {
		if respectsPredicates(nodes[i], predicates...) {
			filtered = append(filtered, nodes[i])
		}
	}
	return filtered
}

// respectsPredicates 对节点应用全部谓词，任一不满足即返回 false（短路求值）。
func respectsPredicates(node *v1.Node, predicates ...NodeConditionPredicate) bool {
	for _, p := range predicates {
		if !p(node) {
			return false
		}
	}
	return true
}
