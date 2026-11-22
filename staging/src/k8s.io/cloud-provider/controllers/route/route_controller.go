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

package route

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	clientretry "k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	cloudprovider "k8s.io/cloud-provider"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/controller-manager/pkg/features"
)

const (
	// Maximal number of concurrent route operation API calls.
	// TODO: This should be per-provider.
	//
	// 中文说明：并发上限用于保护云 API 配额；不同云厂商的限流差异很大，
	// 因此这里取一个保守的全局值，后续应改为按 provider 配置。
	maxConcurrentRouteOperations int = 200
	// In the scenarios with a burst of node events, we don't want to be worse
	// than a 10s interval.
	// The 10s interval was defined in KEP 5237, it is equivalent to the default
	// sync period of the route controller.
	minRouteResyncInterval time.Duration = 10 * time.Second

	// routesSyncKey and routeCorrectionsKey are workqueue keys that both trigger a
	// full-cluster route reconcile. They identify the trigger reason so we can
	// attribute the outcome.
	//
	// 中文说明：两个 key 都代表“全量对账”，分开只是为了让指标能区分触发来源：
	// node_change（节点事件）与 periodic（周期性清理）。
	routesSyncKey       = "node_change"
	routeCorrectionsKey = "periodic"
)

// updateNetworkConditionBackoff 是更新 NodeNetworkUnavailable condition 时的冲突重试策略。
var updateNetworkConditionBackoff = wait.Backoff{
	Steps:    5, // Maximum number of retries.
	Duration: 100 * time.Millisecond,
	Jitter:   1.0,
}

// RouteController 同步节点 PodCIDR 与云侧路由。
//
// 中文说明：reconcile 以整个集群为单位，因此不需要按节点分片的 worker；
// 开启 watch-based 功能时由 workqueue 接收节点事件，否则按固定周期触发。
type RouteController struct {
	routes           cloudprovider.Routes
	kubeClient       clientset.Interface
	clusterName      string
	clusterCIDRs     []*net.IPNet
	nodeInformer     coreinformers.NodeInformer
	nodeLister       corelisters.NodeLister
	nodeListerSynced cache.InformerSynced
	broadcaster      record.EventBroadcaster
	recorder         record.EventRecorder
	workqueue        workqueue.TypedRateLimitingInterface[string]
}

// New 构造 RouteController，并在启用 CloudControllerManagerWatchBasedRoutesReconciliation
// 特性门控时注册节点事件处理器。
func New(
	routes cloudprovider.Routes,
	kubeClient clientset.Interface,
	nodeInformer coreinformers.NodeInformer,
	clusterName string,
	clusterCIDRs []*net.IPNet,
) (*RouteController, error) {
	registerMetrics()

	if len(clusterCIDRs) == 0 {
		// 没有 clusterCIDR 就无法判断哪些路由归本集群，继续运行只会误删他人路由。
		klog.Fatal("RouteController: Must specify clusterCIDR.")
	}

	rc := &RouteController{
		routes:           routes,
		kubeClient:       kubeClient,
		clusterName:      clusterName,
		clusterCIDRs:     clusterCIDRs,
		nodeInformer:     nodeInformer,
		nodeLister:       nodeInformer.Lister(),
		nodeListerSynced: nodeInformer.Informer().HasSynced,
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.CloudControllerManagerWatchBasedRoutesReconciliation) {
		// We use [TypedWithMaxWaitRateLimiter] to cap the [TypedBucketRateLimiter] at 10 seconds.
		// Without this cap, the bucket rate limiter would keep accumulating delay for each node event.
		// With the cap, even under a constant stream of node events, reconciles happen at most every
		// 10 seconds — either triggered immediately or queued for another run if one is already in progress.
		//
		// 中文说明：桶限流器会让连续事件不断累加延迟；套一层 MaxWait 后，无论事件多密集，
		// 对账间隔都不会超过 10 秒，避免新节点长时间得不到路由。
		bucketRateLimiter := &workqueue.TypedBucketRateLimiter[string]{Limiter: rate.NewLimiter(rate.Every(minRouteResyncInterval), 1)}
		rc.workqueue = workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedWithMaxWaitRateLimiter[string](bucketRateLimiter, minRouteResyncInterval),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "Routes"},
		)

		_, err := rc.nodeInformer.Informer().AddEventHandler(
			// It is only necessary to reconcile the routes for any events that have the potential to impact them:
			// - Node is added
			// - Node is removed
			// - Node .Status.Addresses is changed
			// - Node .Spec.PodCIDRs is changed
			cache.ResourceEventHandlerFuncs{
				AddFunc:    rc.enqueueClusterReconcile,
				UpdateFunc: rc.handleNodeUpdate,
				DeleteFunc: rc.enqueueClusterReconcile,
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return rc, nil
}

// enqueueClusterReconcile enqueues a route reconciliation of the cluster.
func (rc *RouteController) enqueueClusterReconcile(_ interface{}) {
	rc.workqueue.AddRateLimited(routesSyncKey)
}

// handleNodeUpdate 只在节点地址或 PodCIDR 变化时入队，过滤掉高频的状态更新事件。
func (rc *RouteController) handleNodeUpdate(oldObj, newObj interface{}) {
	oldNode, oldOk := oldObj.(*v1.Node)
	newNode, newOk := newObj.(*v1.Node)

	if !oldOk || !newOk {
		// 类型断言失败通常意味着 informer 配置有误，忽略即可。
		return
	}

	diffInPodCIDR := !reflect.DeepEqual(oldNode.Spec.PodCIDRs, newNode.Spec.PodCIDRs)
	diffInNodeAddresses := !reflect.DeepEqual(oldNode.Status.Addresses, newNode.Status.Addresses)

	if diffInPodCIDR || diffInNodeAddresses {
		rc.workqueue.AddRateLimited(routesSyncKey)
		return
	}

	// The Node informer triggers a periodic update event.
	// In these cases, the old and new Node objects are identical. We use this event as a signal to perform
	// a route reconciliation — our regular cleanup process — as described in the KEP 5237.
	// We use a separate key to measure the amount of route reconciles due to a
	// periodic reconcile. This is tracked in the metric
	// `route_controller_route_sync_total` via the `trigger="periodic"` label.
	//
	// 中文说明：resync 事件的新旧对象完全相同，借此充当周期性清理的触发器，
	// 从而在没有独立定时器的情况下仍能回收孤儿路由。
	if oldNode.GetResourceVersion() == newNode.GetResourceVersion() {
		rc.workqueue.AddRateLimited(routeCorrectionsKey)
	}
}

// Run 启动 route controller。启用 watch-based 特性时消费 workqueue，否则依靠
// syncPeriod 定时全量对账。
func (rc *RouteController) Run(ctx context.Context, syncPeriod time.Duration, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	defer utilruntime.HandleCrash()

	if utilfeature.DefaultFeatureGate.Enabled(features.CloudControllerManagerWatchBasedRoutesReconciliation) && rc.workqueue != nil {
		defer rc.workqueue.ShutDown()
	}

	rc.broadcaster = record.NewBroadcaster(record.WithContext(ctx))
	rc.recorder = rc.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "route_controller"})

	// Start event processing pipeline.
	if rc.broadcaster != nil {
		rc.broadcaster.StartStructuredLogging(0)
		rc.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: rc.kubeClient.CoreV1().Events("")})
		defer rc.broadcaster.Shutdown()
	}

	klog.Info("Starting route controller")
	defer klog.Info("Shutting down route controller")
	controllerManagerMetrics.ControllerStarted("route")
	defer controllerManagerMetrics.ControllerStopped("route")

	if !cache.WaitForNamedCacheSyncWithContext(ctx, rc.nodeListerSynced) {
		return
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.CloudControllerManagerWatchBasedRoutesReconciliation) {
		// Process any node events that may change the routes.
		// It does not make sense to add concurrency here, as the routes
		// controller always processes the whole cluster.
		go wait.UntilWithContext(ctx, rc.runWorker, time.Second)
	} else {
		go wait.NonSlidingUntil(func() {
			routesChanged, err := rc.reconcileNodeRoutes(ctx)
			// Without the workqueue, reconciles are only driven by the
			// syncPeriod timer, so the trigger is always periodic.
			recordRouteSync(routesChanged, err, "periodic")
			if err != nil {
				klog.Errorf("Couldn't reconcile node routes: %v", err)
				return
			}
		}, syncPeriod, ctx.Done())
	}

	<-ctx.Done()
}

func (rc *RouteController) runWorker(ctx context.Context) {
	for rc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling reconcileNodeRoutes.
func (rc *RouteController) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := rc.workqueue.Get()
	if shutdown {
		return false
	}

	// Forget is a no-op with a BucketRateLimiter. This is still called in case the implementation detail changes,
	// as the [workqueue.TypedRateLimitingInterface] insists on it being called.
	//
	// We call it before reconcileNodeRoutes, as otherwise we may have a race condition:
	// 1. we start a reconcile with a fixed list of nodes
	// 2. the event for a new node is enqueued, but it is not considered in the running reconcile
	// 3. we finish the reconcile and "forget" the event from our queue
	// => The new node is never reconciled
	//
	// 中文说明：必须在开始对账前 Forget，否则对账期间入队的新节点事件会在结束时被
	// 一并清除，导致该节点永远得不到路由。
	rc.workqueue.Forget(obj)

	// We wrap this block in a func so we can defer rc.workqueue.Done.
	err := func(key string) error {
		defer rc.workqueue.Done(key)

		// Run the route reconciliation
		routesChanged, err := rc.reconcileNodeRoutes(ctx)
		recordRouteSync(routesChanged, err, key)
		if err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			rc.workqueue.AddRateLimited(key)
			klog.Infof("Couldn't reconcile node routes: %v, requeuing", err)
			return fmt.Errorf("couldn't reconcile node routes: %w, requeuing", err)
		}

		return nil
	}(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// reconcileNodeRoutes 拉取云侧路由与节点列表，执行一次全量对账。
func (rc *RouteController) reconcileNodeRoutes(ctx context.Context) (bool, error) {
	routeList, err := rc.routes.ListRoutes(ctx, rc.clusterName)
	if err != nil {
		return false, fmt.Errorf("error listing routes: %w", err)
	}
	nodes, err := rc.nodeLister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("error listing nodes: %w", err)
	}
	return rc.reconcile(ctx, nodes, routeList)
}

// routeAction 表示对某条 (node, podCIDR) 路由应采取的动作。
type routeAction string

var (
	keep   routeAction = "keep"
	add    routeAction = "add"
	remove routeAction = "remove"
	update routeAction = "update"
)

// routeNode 汇总单个节点在本次对账中的已知地址、现有路由与待执行动作。
type routeNode struct {
	name            types.NodeName
	addrs           []v1.NodeAddress
	routes          []*cloudprovider.Route
	cidrWithActions *map[string]routeAction
}

// reconcile 是最核心的对账函数：先算出每个 (node, cidr) 的动作，再并发删除/创建
// 路由，最后回写 NodeNetworkUnavailable condition。
//
// 中文说明：删除与创建分两个阶段，且当云厂商支持 EnableNodeAddresses 时先等删除
// 全部完成——否则 action=update 的路由可能被自己在同一轮里重复创建（#98359）。
func (rc *RouteController) reconcile(ctx context.Context, nodes []*v1.Node, routes []*cloudprovider.Route) (bool, error) {
	var l sync.Mutex
	// routeMap includes info about a target Node and its addresses, routes and a map between Pod CIDRs and actions.
	// If action is add/remove, the route will be added/removed.
	// If action is keep, the route will not be touched.
	// If action is update, the route will be deleted and then added.
	routeMap := make(map[types.NodeName]routeNode)
	var routesChanged atomic.Bool

	// Put current routes into routeMap.
	for _, route := range routes {
		if route.TargetNode == "" {
			// 没有目标节点的路由（例如外部管理的路由）不归本控制器处理。
			continue
		}
		rn, ok := routeMap[route.TargetNode]
		if !ok {
			rn = routeNode{
				name:            route.TargetNode,
				addrs:           []v1.NodeAddress{},
				routes:          []*cloudprovider.Route{},
				cidrWithActions: &map[string]routeAction{},
			}
		} else if rn.routes == nil {
			rn.routes = []*cloudprovider.Route{}
		}
		rn.routes = append(rn.routes, route)
		routeMap[route.TargetNode] = rn
	}

	wg := sync.WaitGroup{}
	// rateLimiter 用带缓冲 channel 实现信号量，限制同时进行的云 API 调用数。
	rateLimiter := make(chan struct{}, maxConcurrentRouteOperations)
	// searches existing routes by node for a matching route

	// Check Nodes and their Pod CIDRs. Then put expected route actions into nodePodCIDRActionMap.
	// Add addresses of Nodes into routeMap.
	for _, node := range nodes {
		// Skip if the node hasn't been assigned a CIDR yet.
		if len(node.Spec.PodCIDRs) == 0 {
			continue
		}
		nodeName := types.NodeName(node.Name)
		l.Lock()
		rn, ok := routeMap[nodeName]
		if !ok {
			rn = routeNode{
				name:            nodeName,
				addrs:           []v1.NodeAddress{},
				routes:          []*cloudprovider.Route{},
				cidrWithActions: &map[string]routeAction{},
			}
		}
		rn.addrs = node.Status.Addresses
		routeMap[nodeName] = rn
		l.Unlock()
		// for every node, for every cidr
		for _, podCIDR := range node.Spec.PodCIDRs {
			// we add it to our nodeCIDRs map here because if we don't consider Node addresses change,
			// add and delete go routines run simultaneously.
			l.Lock()
			action := getRouteAction(rn.routes, podCIDR, nodeName, node.Status.Addresses)
			(*routeMap[nodeName].cidrWithActions)[podCIDR] = action
			l.Unlock()
			klog.Infof("action for Node %q with CIDR %q: %q", nodeName, podCIDR, action)
		}
	}

	// searches our bag of node -> cidrs for a match
	// If the action doesn't exist, action is remove or update, then the route should be deleted.
	shouldDeleteRoute := func(nodeName types.NodeName, cidr string) bool {
		l.Lock()
		defer l.Unlock()

		cidrWithActions := routeMap[nodeName].cidrWithActions
		if cidrWithActions == nil {
			// 节点已不在集群中，其路由全部视为孤儿。
			return true
		}
		action, exist := (*cidrWithActions)[cidr]
		if !exist || action == remove || action == update {
			klog.Infof("route should be deleted, spec: exist: %v, action: %q, Node %q, CIDR %q", exist, action, nodeName, cidr)
			return true
		}
		return false
	}

	// remove routes that are not in use or need to be updated.
	for _, route := range routes {
		if !rc.isResponsibleForRoute(route) {
			// 不属于本集群 CIDR 的路由交给其他集群/系统管理。
			continue
		}
		// Check if this route is a blackhole, or applies to a node we know about & CIDR status is created.
		if route.Blackhole || shouldDeleteRoute(route.TargetNode, route.DestinationCIDR) {
			wg.Add(1)
			// Delete the route.
			go func(route *cloudprovider.Route, startTime time.Time) {
				defer wg.Done()
				// respect the rate limiter
				rateLimiter <- struct{}{}
				klog.Infof("Deleting route %s %s", route.Name, route.DestinationCIDR)
				if err := rc.routes.DeleteRoute(ctx, rc.clusterName, route); err != nil {
					// 删除失败只记录：下一轮对账会再次尝试，避免阻塞其他路由。
					klog.Errorf("Could not delete route %s %s after %v: %v", route.Name, route.DestinationCIDR, time.Since(startTime), err)
				} else {
					routesChanged.Store(true)
					klog.Infof("Deleted route %s %s after %v", route.Name, route.DestinationCIDR, time.Since(startTime))
				}
				<-rateLimiter
			}(route, time.Now())
		}
	}
	// https://github.com/kubernetes/kubernetes/issues/98359
	// When routesUpdated is true, Route addition and deletion cannot run simultaneously because if action is update,
	// the same route may be added and deleted.
	if len(routes) != 0 && routes[0].EnableNodeAddresses {
		wg.Wait()
	}

	// Now create new routes or update existing ones.
	for _, node := range nodes {
		// Skip if the node hasn't been assigned a CIDR yet.
		if len(node.Spec.PodCIDRs) == 0 {
			continue
		}
		nodeName := types.NodeName(node.Name)

		// for every node, for every cidr
		for _, podCIDR := range node.Spec.PodCIDRs {
			l.Lock()
			action := (*routeMap[nodeName].cidrWithActions)[podCIDR]
			l.Unlock()
			if action == keep || action == remove {
				continue
			}
			// if we are here, then a route needs to be created for this node
			route := &cloudprovider.Route{
				TargetNode:          nodeName,
				TargetNodeAddresses: node.Status.Addresses,
				DestinationCIDR:     podCIDR,
			}
			klog.Infof("route spec to be created: %v", route)
			// cloud providers that:
			// - depend on nameHint
			// - trying to support dual stack
			// will have to carefully generate new route names that allow node->(multi cidr)
			// 中文说明：用 Node UID 作为 nameHint 的稳定来源；双栈时同一节点有多条路由，
			// 云厂商需在 nameHint 基础上自行区分，不能假设一节点一路由。
			nameHint := string(node.UID)
			wg.Add(1)
			go func(nodeName types.NodeName, nameHint string, route *cloudprovider.Route) {
				defer wg.Done()

				createRouteFunc := func() error {
					startTime := time.Now()
					// Ensure that we don't have more than maxConcurrentRouteOperations
					// CreateRoute calls in flight.
					rateLimiter <- struct{}{}
					klog.Infof("Creating route for node %s %s with hint %s, throttled %v", nodeName, route.DestinationCIDR, nameHint, time.Since(startTime))
					err := rc.routes.CreateRoute(ctx, rc.clusterName, nameHint, route)
					<-rateLimiter
					if err != nil {
						msg := fmt.Sprintf("Could not create route %s %s for node %s after %v: %v", nameHint, route.DestinationCIDR, nodeName, time.Since(startTime), err)
						if rc.recorder != nil {
							rc.recorder.Eventf(
								&v1.ObjectReference{
									APIVersion: "v1",
									Kind:       "Node",
									Name:       string(nodeName),
									UID:        node.UID,
									Namespace:  "",
								}, v1.EventTypeWarning, "FailedToCreateRoute", "%s", msg,
							)
							klog.V(4).Info(msg)
							return err
						}
					}
					routesChanged.Store(true)
					l.Lock()
					// Mark the route action as done (keep)
					// 中文说明：创建成功后把动作改为 keep，后续汇总 NodeNetworkUnavailable 时
					// 才能判断该 CIDR 已就绪。
					(*routeMap[nodeName].cidrWithActions)[route.DestinationCIDR] = keep
					l.Unlock()
					klog.Infof("Created route for node %s %s with hint %s after %v", nodeName, route.DestinationCIDR, nameHint, time.Since(startTime))
					return nil
				}

				// 云厂商更新路由时可能返回冲突（例如并发修改同一路由表），有限重试。
				err := clientretry.RetryOnConflict(updateNetworkConditionBackoff, createRouteFunc)
				if err != nil {
					klog.Errorf("Could not create route %s %s for node %s: %v", nameHint, route.DestinationCIDR, nodeName, err)
				}
			}(nodeName, nameHint, route)
		}
	}
	wg.Wait()

	// after all route actions have been done (or not), we start updating
	// all nodes' statuses with the outcome
	for _, node := range nodes {
		actions := routeMap[types.NodeName(node.Name)].cidrWithActions
		if actions == nil {
			continue
		}

		wg.Add(1)
		if len(*actions) == 0 {
			// 没有任何 PodCIDR 的节点视为路由未就绪。
			go func(n *v1.Node) {
				defer wg.Done()
				klog.Infof("node %v has no routes assigned to it. NodeNetworkUnavailable will be set to true", n.Name)
				if err := rc.updateNetworkingCondition(n, false); err != nil {
					klog.Errorf("failed to update networking condition when no actions: %v", err)
				}
			}(node)
			continue
		}

		// check if all route actions were done. if so, then it should be ready
		allRoutesCreated := true
		for _, action := range *actions {
			if action == add || action == update {
				allRoutesCreated = false
				break
			}
		}
		go func(n *v1.Node) {
			defer wg.Done()
			if err := rc.updateNetworkingCondition(n, allRoutesCreated); err != nil {
				klog.Errorf("failed to update networking condition: %v", err)
			}
		}(node)
	}
	wg.Wait()

	return routesChanged.Load(), nil
}

// updateNetworkingCondition 幂等地维护 Node 的 NodeNetworkUnavailable condition。
func (rc *RouteController) updateNetworkingCondition(node *v1.Node, routesCreated bool) error {
	_, condition := nodeutil.GetNodeCondition(&node.Status, v1.NodeNetworkUnavailable)
	// 已是目标状态时不重复写，减少 apiserver 压力。
	if routesCreated && condition != nil && condition.Status == v1.ConditionFalse && condition.Reason == "RouteCreated" {
		klog.V(2).Infof("set node %v with NodeNetworkUnavailable=false was canceled because it is already set", node.Name)
		return nil
	}

	if !routesCreated && condition != nil && condition.Status == v1.ConditionTrue && condition.Reason == "NoRouteCreated" {
		klog.V(2).Infof("set node %v with NodeNetworkUnavailable=true was canceled because it is already set", node.Name)
		return nil
	}

	klog.Infof("Patching node status %v with %v previous condition was:%+v", node.Name, routesCreated, condition)

	// either condition is not there, or has a value != to what we need
	// start setting it
	err := clientretry.RetryOnConflict(updateNetworkConditionBackoff, func() error {
		var err error
		// Patch could also fail, even though the chance is very slim. So we still do
		// patch in the retry loop.
		currentTime := metav1.Now()
		if routesCreated {
			err = nodeutil.SetNodeCondition(rc.kubeClient, types.NodeName(node.Name), v1.NodeCondition{
				Type:               v1.NodeNetworkUnavailable,
				Status:             v1.ConditionFalse,
				Reason:             "RouteCreated",
				Message:            "RouteController created a route",
				LastTransitionTime: currentTime,
			})
		} else {
			err = nodeutil.SetNodeCondition(rc.kubeClient, types.NodeName(node.Name), v1.NodeCondition{
				Type:               v1.NodeNetworkUnavailable,
				Status:             v1.ConditionTrue,
				Reason:             "NoRouteCreated",
				Message:            "RouteController failed to create a route",
				LastTransitionTime: currentTime,
			})
		}
		if err != nil {
			klog.V(4).Infof("Error updating node %s, retrying: %v", types.NodeName(node.Name), err)
		}
		return err
	})
	if err != nil {
		klog.Errorf("Error updating node %s: %v", node.Name, err)
	}

	return err
}

// isResponsibleForRoute 判断路由的目标 CIDR 是否落在本集群的 clusterCIDR 内。
// 中文说明：云侧路由表中可能同时存在多个集群的路由，只有与本集群 CIDR 有交集的
// 才允许删除/修改，防止误删其他集群的条目。
func (rc *RouteController) isResponsibleForRoute(route *cloudprovider.Route) bool {
	_, cidr, err := netutils.ParseCIDRSloppy(route.DestinationCIDR)
	if err != nil {
		// 无法解析的 CIDR 不可能是我们自己创建的，直接跳过。
		klog.Errorf("Ignoring route %s, unparsable CIDR: %v", route.Name, err)
		return false
	}
	// Not responsible if this route's CIDR is not within our clusterCIDR
	// lastIP 是子网的广播地址；判断首尾任一地址是否落在 clusterCIDR 内即可覆盖
	// 路由 CIDR 与 clusterCIDR 部分重叠的情况。
	lastIP := make([]byte, len(cidr.IP))
	for i := range lastIP {
		lastIP[i] = cidr.IP[i] | ^cidr.Mask[i]
	}

	// check across all cluster cidrs
	for _, clusterCIDR := range rc.clusterCIDRs {
		if clusterCIDR.Contains(cidr.IP) || clusterCIDR.Contains(lastIP) {
			return true
		}
	}
	return false
}

// getRouteAction returns an action according to if there's a route matches a specific cidr and target Node addresses.
//
// 中文说明：destination 相同即视为同一条路由；只有当云厂商支持节点地址（EnableNodeAddresses）
// 且地址发生变化时才需要 update，否则地址变化无法反映到云侧，保持 keep。
func getRouteAction(routes []*cloudprovider.Route, cidr string, nodeName types.NodeName, realNodeAddrs []v1.NodeAddress) routeAction {
	for _, route := range routes {
		if route.DestinationCIDR == cidr {
			if !route.EnableNodeAddresses || equalNodeAddrs(realNodeAddrs, route.TargetNodeAddresses) {
				return keep
			}
			klog.Infof("Node addresses have changed from %v to %v", route.TargetNodeAddresses, realNodeAddrs)
			return update
		}
	}
	return add
}

// equalNodeAddrs 忽略顺序比较两组节点地址是否等价。
// 中文说明：云厂商返回地址顺序不稳定，按集合语义比较可避免每轮误判为 update。
func equalNodeAddrs(addrs0 []v1.NodeAddress, addrs1 []v1.NodeAddress) bool {
	if len(addrs0) != len(addrs1) {
		return false
	}
	for _, ip0 := range addrs0 {
		found := false
		for _, ip1 := range addrs1 {
			if reflect.DeepEqual(ip0, ip1) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
