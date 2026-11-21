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

// 本控制器为节点设置污点（taint）：带污点的节点不应承载新的工作负载，
// 并且应尽量将已有工作负载从这些节点上驱逐。

package nodelifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"

	coordv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
	coordinformers "k8s.io/client-go/informers/coordination/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	coordlisters "k8s.io/client-go/listers/coordination/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	nodetopology "k8s.io/component-helpers/node/topology"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/nodelifecycle/scheduler"
	"k8s.io/kubernetes/pkg/controller/tainteviction"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	controllerutil "k8s.io/kubernetes/pkg/controller/util/node"
	"k8s.io/kubernetes/pkg/features"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
)

// init 在包加载时注册 Prometheus 指标。
func init() {
	// 包初始化时注册 Prometheus 指标。
	Register()
}

// 以下模板与映射把节点状态转换为对应污点，供状态调和时复用。
var (
	// UnreachableTaintTemplate 是节点失联（Ready=Unknown）时添加的污点模板。
	UnreachableTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeUnreachable,
		Effect: v1.TaintEffectNoExecute,
	}

	// NotReadyTaintTemplate 是节点未就绪（Ready=False）时添加的污点模板。
	NotReadyTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeNotReady,
		Effect: v1.TaintEffectNoExecute,
	}

	// 映射结构为 {节点条件类型: {条件状态: 污点键}}：当某节点条件处于特定状态时，
	// 应为节点打上对应的污点。同一种条件类型可能对应多个 {状态, 污点键} 组合。
	nodeConditionToTaintKeyStatusMap = map[v1.NodeConditionType]map[v1.ConditionStatus]string{
		v1.NodeReady: {
			v1.ConditionFalse:   v1.TaintNodeNotReady,
			v1.ConditionUnknown: v1.TaintNodeUnreachable,
		},
		v1.NodeMemoryPressure: {
			v1.ConditionTrue: v1.TaintNodeMemoryPressure,
		},
		v1.NodeDiskPressure: {
			v1.ConditionTrue: v1.TaintNodeDiskPressure,
		},
		v1.NodeNetworkUnavailable: {
			v1.ConditionTrue: v1.TaintNodeNetworkUnavailable,
		},
		v1.NodePIDPressure: {
			v1.ConditionTrue: v1.TaintNodePIDPressure,
		},
	}

	// taintKeyToNodeConditionMap 是反向映射：由控制器管理的污点键查回对应的节点条件类型。
	taintKeyToNodeConditionMap = map[string]v1.NodeConditionType{
		v1.TaintNodeNotReady:           v1.NodeReady,
		v1.TaintNodeUnreachable:        v1.NodeReady,
		v1.TaintNodeNetworkUnavailable: v1.NodeNetworkUnavailable,
		v1.TaintNodeMemoryPressure:     v1.NodeMemoryPressure,
		v1.TaintNodeDiskPressure:       v1.NodeDiskPressure,
		v1.TaintNodePIDPressure:        v1.NodePIDPressure,
	}

	leaseResource = coordv1.SchemeGroupVersion.WithResource("leases").GroupResource()
)

// ZoneState 描述可用区的整体健康状态。
type ZoneState string

// 可用区状态取值：初始、正常、完全中断与部分中断。
const (
	stateInitial           = ZoneState("Initial")
	stateNormal            = ZoneState("Normal")
	stateFullDisruption    = ZoneState("FullDisruption")
	statePartialDisruption = ZoneState("PartialDisruption")
)

// 控制器内部使用的重试间隔与更新 worker 数量常量。
const (
	// 重试节点健康状态更新时的休眠间隔。
	retrySleepTime = 20 * time.Millisecond
	// 大多数 Pod 由 monitorNodeHealth 巡检处理；Pod update worker 只处理缓存滞后的 Pod，4 个足够。
	podUpdateWorkerSize = 4
	// nodeUpdateWorkerSize 定义处理节点/Pod 更新的 worker 数量。
	nodeUpdateWorkerSize = 8

	// 在此定义而非从 cmd/kube-controller-manager/names 导入，是为了避免引入依赖导致校验错误。
	// SeparateTaintEvictionController 特性毕业（GA）后将移除该常量。
	taintEvictionController = "taint-eviction-controller"
)

// labelReconcileInfo 列出需要调和的节点标签及调和方式，primaryKey 与 secondaryKey 为标签键：
//   - 两个标签都存在但值不一致时，以 primaryKey 的值为准进行调和；
//   - ensureSecondaryExists 为 true 且 secondaryKey 不存在时，用 primaryKey 的值补齐。
var labelReconcileInfo = []struct {
	primaryKey            string
	secondaryKey          string
	ensureSecondaryExists bool
}{
	{
		// 以稳定版标签为准调和 beta 版操作系统标签。
		// TODO(#89477): no earlier than 1.22: drop the beta labels if they differ from the GA labels
		primaryKey:            v1.LabelOSStable,
		secondaryKey:          kubeletapis.LabelOS,
		ensureSecondaryExists: true,
	},
	{
		// 以稳定版标签为准调和 beta 版架构标签。
		// TODO(#89477): no earlier than 1.22: drop the beta labels if they differ from the GA labels
		primaryKey:            v1.LabelArchStable,
		secondaryKey:          kubeletapis.LabelArch,
		ensureSecondaryExists: true,
	},
}

// nodeHealthData 保存某节点最近一次被观测到的健康状态与本地观测时间戳。
type nodeHealthData struct {
	probeTimestamp           metav1.Time
	readyTransitionTimestamp metav1.Time
	status                   *v1.NodeStatus
	lease                    *coordv1.Lease
}

// deepCopy 返回健康数据的深拷贝，避免调用方修改共享状态。
func (n *nodeHealthData) deepCopy() *nodeHealthData {
	if n == nil {
		return nil
	}
	return &nodeHealthData{
		probeTimestamp:           n.probeTimestamp,
		readyTransitionTimestamp: n.readyTransitionTimestamp,
		status:                   n.status.DeepCopy(),
		lease:                    n.lease.DeepCopy(),
	}
}

// nodeHealthMap 是线程安全的节点健康数据表。
type nodeHealthMap struct {
	lock        sync.RWMutex
	nodeHealths map[string]*nodeHealthData
}

// newNodeHealthMap 创建一个空的节点健康数据表。
func newNodeHealthMap() *nodeHealthMap {
	return &nodeHealthMap{
		nodeHealths: make(map[string]*nodeHealthData),
	}
}

// getDeepCopy 返回节点健康数据的副本，防止从 map 取出后原始数据被并发修改。
func (n *nodeHealthMap) getDeepCopy(name string) *nodeHealthData {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.nodeHealths[name].deepCopy()
}

// set 写入节点健康数据（持写锁）。
func (n *nodeHealthMap) set(name string, data *nodeHealthData) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.nodeHealths[name] = data
}

// podUpdateItem 是 Pod 更新队列中用于定位 Pod 的命名空间/名称二元组。
type podUpdateItem struct {
	namespace string
	name      string
}

// Controller 管理节点生命周期：监控 kubelet 上报的健康信号，维护节点污点并处理驱逐节奏。
type Controller struct {
	taintManager *tainteviction.Controller

	podLister         corelisters.PodLister
	podInformerSynced cache.InformerSynced
	kubeClient        clientset.Interface

	// 使用该本地时间戳代替 Condition 中的 LastProbeTime，以避免集群各节点间时钟偏移带来的误判。
	now func() metav1.Time

	enterPartialDisruptionFunc func(nodeNum int) float32
	enterFullDisruptionFunc    func(nodeNum int) float32
	computeZoneStateFunc       func(nodeConditions []*v1.NodeCondition) (int, ZoneState)

	knownNodeSet map[string]*v1.Node
	// 按节点保存最近一次观测到的健康状态及本地观测时间。
	nodeHealthMap *nodeHealthMap

	// evictorLock 保护 zoneNoExecuteTainter 的并发访问。
	evictorLock sync.Mutex
	// 按可用区划分的限速队列，负责为节点添加 NoExecute 污点。
	zoneNoExecuteTainter map[string]*scheduler.RateLimitedTimedQueue

	nodesToRetry sync.Map

	zoneStates map[string]ZoneState

	daemonSetStore          appsv1listers.DaemonSetLister
	daemonSetInformerSynced cache.InformerSynced

	leaseLister         coordlisters.LeaseLister
	leaseInformerSynced cache.InformerSynced

	consistencyStore   consistencyutil.ConsistencyStore
	nodeLister         corelisters.NodeLister
	nodeInformerSynced cache.InformerSynced

	getPodsAssignedToNode func(nodeName string) ([]*v1.Pod, error)

	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder

	// 控制器检查 kubelet 健康信号的周期，应小于 nodeMonitorGracePeriod。
	// TODO: Change node health monitor to watch based.
	nodeMonitorPeriod time.Duration

	// 节点刚创建时（例如集群启动或扩容）给予更长的宽限期，避免因启动慢被误判为失联。
	nodeStartupGracePeriod time.Duration

	// 控制器不主动同步节点健康，而是监控 kubelet 上报的两种健康信号（NodeStatus 与 NodeLease）。
	// 若超过该时长未收到更新，则将节点状态置为 NodeReady==ConditionUnknown。
	// 修改该值需谨慎：它必须与 kubelet 的 nodeStatusUpdateFrequency 及 NodeLease 的
	// renewInterval 协同，健康信号实际更新频率取两者中较小者。约束如下：
	// 1. 宽限期至少是健康信号更新频率的 N 倍（N 为允许的重试次数）；小于更新频率没有意义，
	//    因为该间隔内不会有新的心跳。
	// 2. 宽限期应大于 http2 健康检查的 HTTP2_PING_TIMEOUT_SECONDS(30s) 与
	//    HTTP2_READ_IDLE_TIMEOUT_SECONDS(15s) 之和，确保服务端有机会处理缓慢或空闲连接。
	// 3. 宽限期也不能过大，否则用户看到节点健康状态变化会有明显延迟。
	nodeMonitorGracePeriod time.Duration

	// 处理节点健康更新的 worker 数量，默认取 scheduler.UpdateWorkerSize。
	nodeUpdateWorkerSize int

	evictionLimiterQPS          float32
	secondaryEvictionLimiterQPS float32
	largeClusterThreshold       int32
	unhealthyZoneThreshold      float32

	nodeUpdateQueue workqueue.TypedInterface[string]
	podUpdateQueue  workqueue.TypedRateLimitingInterface[podUpdateItem]
}

// NewNodeLifecycleController 构造节点生命周期控制器，注册 Pod/Node/Lease 事件处理器并初始化各限速队列。
func NewNodeLifecycleController(
	ctx context.Context,
	leaseInformer coordinformers.LeaseInformer,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	daemonSetInformer appsv1informers.DaemonSetInformer,
	kubeClient clientset.Interface,
	nodeMonitorPeriod time.Duration,
	nodeStartupGracePeriod time.Duration,
	nodeMonitorGracePeriod time.Duration,
	evictionLimiterQPS float32,
	secondaryEvictionLimiterQPS float32,
	largeClusterThreshold int32,
	unhealthyZoneThreshold float32,
) (*Controller, error) {
	logger := klog.FromContext(ctx)
	if kubeClient == nil {
		logger.Error(nil, "kubeClient is nil when starting nodelifecycle Controller")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "node-controller"})

	nc := &Controller{
		kubeClient:                  kubeClient,
		now:                         metav1.Now,
		knownNodeSet:                make(map[string]*v1.Node),
		nodeHealthMap:               newNodeHealthMap(),
		broadcaster:                 eventBroadcaster,
		recorder:                    recorder,
		nodeMonitorPeriod:           nodeMonitorPeriod,
		nodeStartupGracePeriod:      nodeStartupGracePeriod,
		nodeMonitorGracePeriod:      nodeMonitorGracePeriod,
		nodeUpdateWorkerSize:        nodeUpdateWorkerSize,
		zoneNoExecuteTainter:        make(map[string]*scheduler.RateLimitedTimedQueue),
		nodesToRetry:                sync.Map{},
		zoneStates:                  make(map[string]ZoneState),
		evictionLimiterQPS:          evictionLimiterQPS,
		secondaryEvictionLimiterQPS: secondaryEvictionLimiterQPS,
		largeClusterThreshold:       largeClusterThreshold,
		unhealthyZoneThreshold:      unhealthyZoneThreshold,
		nodeUpdateQueue:             workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[string]{Name: "node_lifecycle_controller"}),
		podUpdateQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[podUpdateItem](),
			workqueue.TypedRateLimitingQueueConfig[podUpdateItem]{
				Name: "node_lifecycle_controller_pods",
			},
		),
	}

	nc.enterPartialDisruptionFunc = nc.ReducedQPSFunc
	nc.enterFullDisruptionFunc = nc.HealthyQPSFunc
	nc.computeZoneStateFunc = nc.ComputeZoneState

	// Pod 调度到节点（或改调度）时入队，以便及时标记 Pod 的 NotReady 状态。
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			nc.podUpdated(nil, pod)
		},
		UpdateFunc: func(prev, obj interface{}) {
			prevPod := prev.(*v1.Pod)
			newPod := obj.(*v1.Pod)
			nc.podUpdated(prevPod, newPod)
		},
	})
	nc.podInformerSynced = podInformer.Informer().HasSynced
	// 建立按 nodeName 检索 Pod 的索引，避免每次全量扫描。
	controller.AddPodNodeNameIndexer(podInformer.Informer())
	podIndexer := podInformer.Informer().GetIndexer()
	nc.getPodsAssignedToNode = func(nodeName string) ([]*v1.Pod, error) {
		objs, err := podIndexer.ByIndex(controller.PodNodeNameKeyIndex, nodeName)
		if err != nil {
			return nil, err
		}
		pods := make([]*v1.Pod, 0, len(objs))
		for _, obj := range objs {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				continue
			}
			pods = append(pods, pod)
		}
		return pods, nil
	}
	nc.podLister = podInformer.Lister()
	nc.nodeLister = nodeInformer.Lister()

	if !utilfeature.DefaultFeatureGate.Enabled(features.SeparateTaintEvictionController) {
		logger.Info("Running TaintEvictionController as part of NodeLifecyleController")
		tm, err := tainteviction.New(ctx, kubeClient, podInformer, nodeInformer, taintEvictionController)
		if err != nil {
			return nil, err
		}
		nc.taintManager = tm
	}

	logger.Info("Controller will reconcile labels")
	// 节点增删/更新时将其加入 nodeUpdateQueue，删除时同步清理重试标记。
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controllerutil.CreateAddNodeHandler(func(node *v1.Node) error {
			nc.nodeUpdateQueue.Add(node.Name)
			return nil
		}),
		UpdateFunc: controllerutil.CreateUpdateNodeHandler(func(_, newNode *v1.Node) error {
			nc.nodeUpdateQueue.Add(newNode.Name)
			return nil
		}),
		DeleteFunc: controllerutil.CreateDeleteNodeHandler(logger, func(node *v1.Node) error {
			nc.nodesToRetry.Delete(node.Name)
			return nil
		}),
	})

	nc.leaseLister = leaseInformer.Lister()
	nc.leaseInformerSynced = leaseInformer.Informer().HasSynced

	nc.nodeInformerSynced = nodeInformer.Informer().HasSynced

	nc.daemonSetStore = daemonSetInformer.Lister()
	nc.daemonSetInformerSynced = daemonSetInformer.Informer().HasSynced

	if utilfeature.DefaultFeatureGate.Enabled(features.NodeControllerLeaseCircuitBreaker) {
		nc.consistencyStore = consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
			leaseResource: leaseInformer.Informer().GetStore(),
		})
	} else {
		nc.consistencyStore = consistencyutil.NewNoopConsistencyStore()
	}

	return nc, nil
}

// Run 启动异步循环，持续监控集群节点状态直到 ctx 结束。
func (nc *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	// 启动事件处理管道。
	nc.broadcaster.StartStructuredLogging(3)
	logger := klog.FromContext(ctx)
	logger.Info("Sending events to api server")
	nc.broadcaster.StartRecordingToSink(
		&v1core.EventSinkImpl{
			Interface: v1core.New(nc.kubeClient.CoreV1().RESTClient()).Events(""),
		})
	defer nc.broadcaster.Shutdown()

	logger.Info("Starting node controller")

	// 关闭更新队列以清理 goroutine。
	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down node controller")
		nc.nodeUpdateQueue.ShutDown()
		nc.podUpdateQueue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, nc.leaseInformerSynced, nc.nodeInformerSynced, nc.podInformerSynced, nc.daemonSetInformerSynced) {
		return
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.SeparateTaintEvictionController) {
		logger.Info("Starting", "controller", taintEvictionController)
		wg.Go(func() {
			nc.taintManager.Run(ctx)
		})
	}

	// 启动 worker，负责调和节点标签与 NoSchedule 污点。
	for i := 0; i < nodeUpdateWorkerSize; i++ {
		// workqueue 保证同一 key 在被 Done 前只由一个 worker 处理：取出时会被标记，
		// 期间到达的新事件会在 Done 后重新入队，既不并发处理同一节点也不丢失事件。
		wg.Go(func() {
			wait.UntilWithContext(ctx, nc.doNodeProcessingPassWorker, time.Second)
		})
	}

	for i := 0; i < podUpdateWorkerSize; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, nc.doPodProcessingWorker, time.Second)
		})
	}

	// 处理基于污点的驱逐：TaintManager 不针对本控制器产生的污点做特殊处理，且通常不对
	// 污点驱逐本身限速，因此需要在此对添加污点的动作限速。
	wg.Go(func() {
		wait.UntilWithContext(ctx, nc.doNoExecuteTaintingPass, scheduler.NodeEvictionPeriod)
	})

	// 综合处理 kubelet 上报到控制面的健康信号。
	wg.Go(func() {
		wait.UntilWithContext(ctx, func(ctx context.Context) {
			if err := nc.monitorNodeHealth(ctx); err != nil {
				logger.Error(err, "Error monitoring node health")
			}
		}, nc.nodeMonitorPeriod)
	})
	<-ctx.Done()
}

// doNodeProcessingPassWorker 消费 nodeUpdateQueue，处理节点污点与标签调和。
func (nc *Controller) doNodeProcessingPassWorker(ctx context.Context) {
	logger := klog.FromContext(ctx)
	for {
		obj, shutdown := nc.nodeUpdateQueue.Get()
		// 队列会随 ctx 结束而关闭，通过 shutdown 标志即可退出，无需重复检查 ctx。
		if shutdown {
			return
		}
		nodeName := obj
		if err := nc.doNoScheduleTaintingPass(ctx, nodeName); err != nil {
			logger.Error(err, "Failed to taint NoSchedule on node, requeue it", "node", klog.KRef("", nodeName))
			// TODO(k82cn): Add nodeName back to the queue
		}
		// TODO: re-evaluate whether there are any labels that need to be
		// reconcile in 1.19. Remove this function if it's no longer necessary.
		if err := nc.reconcileNodeLabels(ctx, nodeName); err != nil {
			logger.Error(err, "Failed to reconcile labels for node, requeue it", "node", klog.KRef("", nodeName))
			// TODO(yujuhong): Add nodeName back to the queue
		}
		nc.nodeUpdateQueue.Done(nodeName)
	}
}

// doNoScheduleTaintingPass 根据节点条件与 Unschedulable 标记计算期望的 NoSchedule 污点集合，并与现有污点做差量更新。
func (nc *Controller) doNoScheduleTaintingPass(ctx context.Context, nodeName string) error {
	node, err := nc.nodeLister.Get(nodeName)
	if err != nil {
		// 节点已被删除，忽略。
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// 将节点条件映射为 NoSchedule 污点。
	var taints []v1.Taint
	for _, condition := range node.Status.Conditions {
		if taintMap, found := nodeConditionToTaintKeyStatusMap[condition.Type]; found {
			if taintKey, found := taintMap[condition.Status]; found {
				taints = append(taints, v1.Taint{
					Key:    taintKey,
					Effect: v1.TaintEffectNoSchedule,
				})
			}
		}
	}
	if node.Spec.Unschedulable {
		// 节点被显式标记为不可调度时追加对应污点。
		taints = append(taints, v1.Taint{
			Key:    v1.TaintNodeUnschedulable,
			Effect: v1.TaintEffectNoSchedule,
		})
	}

	// 从节点现有污点中筛出由本控制器管理的 NoSchedule 污点，便于后续做差集。
	nodeTaints := taintutils.TaintSetFilter(node.Spec.Taints, func(t *v1.Taint) bool {
		// 只比较 NoSchedule 污点，避免删除其他控制器添加的污点。
		if t.Effect != v1.TaintEffectNoSchedule {
			return false
		}
		// 保留不可调度污点。
		if t.Key == v1.TaintNodeUnschedulable {
			return true
		}
		// 保留由节点条件映射而来的污点。
		_, found := taintKeyToNodeConditionMap[t.Key]
		return found
	})
	taintsToAdd, taintsToDel := taintutils.TaintSetDiff(taints, nodeTaints)
	// 无增删时避免发起无意义的 API 写请求。
	if len(taintsToAdd) == 0 && len(taintsToDel) == 0 {
		return nil
	}
	if !controllerutil.SwapNodeControllerTaint(ctx, nc.kubeClient, taintsToAdd, taintsToDel, node) {
		return fmt.Errorf("failed to swap taints of node %+v", node)
	}
	return nil
}

// doNoExecuteTaintingPass 从各可用区的限速队列中取出节点，按 Ready 状态为其添加/替换 NoExecute 污点。
func (nc *Controller) doNoExecuteTaintingPass(ctx context.Context) {
	// 先提取键集合，尽量缩短持锁范围，避免整个函数期间占用 evictorLock。
	var zoneNoExecuteTainterKeys []string
	func() {
		nc.evictorLock.Lock()
		defer nc.evictorLock.Unlock()

		zoneNoExecuteTainterKeys = make([]string, 0, len(nc.zoneNoExecuteTainter))
		for k := range nc.zoneNoExecuteTainter {
			zoneNoExecuteTainterKeys = append(zoneNoExecuteTainterKeys, k)
		}
	}()
	logger := klog.FromContext(ctx)
	for _, k := range zoneNoExecuteTainterKeys {
		// 先复制队列指针到局部变量，避免在调用 Try 期间持有 evictorLock。
		var zoneNoExecuteTainterWorker *scheduler.RateLimitedTimedQueue
		func() {
			nc.evictorLock.Lock()
			defer nc.evictorLock.Unlock()
			// 可用区只会新增不会删除，因此对应队列也只会增加不会移除，此处无需检查键是否存在。
			zoneNoExecuteTainterWorker = nc.zoneNoExecuteTainter[k]
		}()
		// Try 的回调返回 false 与重试间隔表示稍后重试，返回 true 表示处理完成。
		zoneNoExecuteTainterWorker.Try(logger, func(value scheduler.TimedValue) (bool, time.Duration) {
			node, err := nc.nodeLister.Get(value.Value)
			if apierrors.IsNotFound(err) {
				logger.Info("Node no longer present in nodeLister", "node", klog.KRef("", value.Value))
				return true, 0
			} else if err != nil {
				logger.Info("Failed to get Node from the nodeLister", "node", klog.KRef("", value.Value), "err", err)
				// 缓存读取失败，50ms 后重试。
				return false, 50 * time.Millisecond
			}
			_, condition := controllerutil.GetNodeCondition(&node.Status, v1.NodeReady)
			if condition == nil {
				logger.Info("Failed to get NodeCondition from the node status", "node", klog.KRef("", value.Value))
				// 缓存读取失败，50ms 后重试。
				return false, 50 * time.Millisecond
			}
			// 为模拟 NodeStatus.Condition["Ready"]，unreachable 与 not-ready 两种污点必须互斥。
			taintToAdd := v1.Taint{}
			oppositeTaint := v1.Taint{}
			switch condition.Status {
			case v1.ConditionFalse:
				taintToAdd = *NotReadyTaintTemplate
				oppositeTaint = *UnreachableTaintTemplate
			case v1.ConditionUnknown:
				taintToAdd = *UnreachableTaintTemplate
				oppositeTaint = *NotReadyTaintTemplate
			default:
				// 节点已恢复就绪，无需处理队列中的污点请求。
				logger.V(4).Info("Node was in a taint queue, but it's ready now. Ignoring taint request", "node", klog.KRef("", value.Value))
				return true, 0
			}
			result := controllerutil.SwapNodeControllerTaint(ctx, nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{&oppositeTaint}, node)
			if result {
				// 污点添加成功时按可用区累计驱逐计数。
				zone := nodetopology.GetZoneKey(node)
				evictionsTotal.WithLabelValues(zone).Inc()
			}

			return result, 0
		})
	}
}

// shortCircuitError 是标记类型，用于通知 monitorNodeHealth 的轮询循环立即带错返回，不再重试当前节点。
type shortCircuitError struct {
	error
}

// Unwrap 暴露底层错误，便于 errors.Is/As 判断。
func (n shortCircuitError) Unwrap() error {
	return n.error
}

// monitorNodeHealth 检查 kubelet 是否持续更新节点健康状态，若长时间未更新则将 NodeReady 置为 Unknown。
// 该函数同时负责：
//   - 将长时间未就绪/失联的节点加入限速队列，由 doNoExecuteTaintingPass 添加 NoExecute 污点；
//   - 根据节点 Ready 条件更新 Pod 的 Ready 状态。
func (nc *Controller) monitorNodeHealth(ctx context.Context) error {
	start := nc.now()
	defer func() {
		updateAllNodesHealthDuration.Observe(time.Since(start.Time).Seconds())
	}()

	// 从本地缓存列出节点：相对 etcd 的短暂延迟可以容忍，且系统本身就是最终一致性的。
	nodes, err := nc.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}
	added, deleted, newZoneRepresentatives := nc.classifyNodes(nodes)
	logger := klog.FromContext(ctx)
	for i := range newZoneRepresentatives {
		nc.addPodEvictorForNewZone(logger, newZoneRepresentatives[i])
	}
	for i := range added {
		logger.V(1).Info("Controller observed a new Node", "node", klog.KRef("", added[i].Name))
		controllerutil.RecordNodeEvent(ctx, nc.recorder, added[i].Name, string(added[i].UID), v1.EventTypeNormal, "RegisteredNode", fmt.Sprintf("Registered Node %v in Controller", added[i].Name))
		nc.knownNodeSet[added[i].Name] = added[i]
		nc.addPodEvictorForNewZone(logger, added[i])
		nc.markNodeAsReachable(ctx, added[i])
	}

	for i := range deleted {
		logger.V(1).Info("Controller observed a Node deletion", "node", klog.KRef("", deleted[i].Name))
		controllerutil.RecordNodeEvent(ctx, nc.recorder, deleted[i].Name, string(deleted[i].UID), v1.EventTypeNormal, "RemovingNode", fmt.Sprintf("Removing Node %v from Controller", deleted[i].Name))
		delete(nc.knownNodeSet, deleted[i].Name)
	}

	// 并发更新节点时会同时写入 zoneToNodeConditions，用单独的锁保护。
	var zoneToNodeConditionsLock sync.Mutex
	zoneToNodeConditions := map[string][]*v1.NodeCondition{}
	updateNodeFunc := func(piece int) {
		start := nc.now()
		defer func() {
			updateNodeHealthDuration.Observe(time.Since(start.Time).Seconds())
		}()

		// 深拷贝节点，因为 tryUpdateNodeHealth 会原地修改条件，不能污染 informer 缓存对象。
		var observedReadyCondition v1.NodeCondition
		var currentReadyCondition *v1.NodeCondition
		node := nodes[piece].DeepCopy()

		if err := wait.PollImmediate(retrySleepTime, retrySleepTime*scheduler.NodeHealthUpdateRetry, func() (bool, error) {
			var err error
			_, observedReadyCondition, currentReadyCondition, err = nc.tryUpdateNodeHealth(ctx, node)
			if err == nil {
				return true, nil
			}
			// 短路错误表示缓存不一致，重试无意义，直接退出轮询。
			if utilfeature.DefaultFeatureGate.Enabled(features.NodeControllerLeaseCircuitBreaker) {
				if err, ok := errors.AsType[shortCircuitError](err); ok {
					return false, err.error
				}
			}
			name := node.Name
			node, err = nc.kubeClient.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				logger.Error(nil, "Failed while getting a Node to retry updating node health. Probably Node was deleted", "node", klog.KRef("", name))
				return false, err
			}
			return false, nil
		}); err != nil {
			logger.Error(err, "Update health of Node from Controller error, Skipping - no pods will be evicted", "node", klog.KObj(node))
			return
		}

		// 部分节点被显式排除在中断检查之外，不应计入可用区状态。
		if !isNodeExcludedFromDisruptionChecks(node) {
			zoneToNodeConditionsLock.Lock()
			zoneToNodeConditions[nodetopology.GetZoneKey(node)] = append(zoneToNodeConditions[nodetopology.GetZoneKey(node)], currentReadyCondition)
			zoneToNodeConditionsLock.Unlock()
		}

		if currentReadyCondition != nil {
			pods, err := nc.getPodsAssignedToNode(node.Name)
			if err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "Unable to list pods of node", node.Name)
				if currentReadyCondition.Status != v1.ConditionTrue && observedReadyCondition.Status == v1.ConditionTrue {
					// 错误恰好发生在 Ready -> NotReady 转换期间，必须标记重试，
					// 确保下一轮仍会执行 MarkPodsNotReady。
					nc.nodesToRetry.Store(node.Name, struct{}{})
				}
				return
			}
			nc.processTaintBaseEviction(ctx, node, currentReadyCondition)

			_, needsRetry := nc.nodesToRetry.Load(node.Name)
			switch {
			case currentReadyCondition.Status != v1.ConditionTrue && observedReadyCondition.Status == v1.ConditionTrue:
				// 仅在状态变化时记录一次事件，避免刷屏。
				controllerutil.RecordNodeStatusChange(logger, nc.recorder, node, "NodeNotReady")
				fallthrough
			case needsRetry && observedReadyCondition.Status != v1.ConditionTrue:
				if err = controllerutil.MarkPodsNotReady(ctx, nc.kubeClient, nc.recorder, pods, node.Name); err != nil {
					utilruntime.HandleErrorWithContext(ctx, err, "Unable to mark all pods NotReady on node; queuing for retry", "node", node.Name)
					nc.nodesToRetry.Store(node.Name, struct{}{})
					return
				}
			}
		}
		nc.nodesToRetry.Delete(node.Name)
	}

	// 标记节点上所有 Pod 为 NotReady 需要逐个更新状态，主要耗时在 IO 等待上；
	// 若节点也串行处理会非常慢，因此这里使用有界并发。
	workqueue.ParallelizeUntil(ctx, nc.nodeUpdateWorkerSize, len(nodes), updateNodeFunc)

	nc.handleDisruption(ctx, zoneToNodeConditions, nodes)

	return nil
}

// processTaintBaseEviction 根据当前 Ready 条件决定节点污点的添加、替换或移除，并进入/退出限速队列。
func (nc *Controller) processTaintBaseEviction(ctx context.Context, node *v1.Node, currentReadyCondition *v1.NodeCondition) {
	decisionTimestamp := nc.now()
	// 以本次判定时刻为基准检查驱逐超时。
	logger := klog.FromContext(ctx)
	switch currentReadyCondition.Status {
	case v1.ConditionFalse:
		// 若节点已被打上 Unreachable 污点，说明已过宽限期，直接替换为 NotReady 污点，无需再次排队等待。
		if taintutils.TaintExists(node.Spec.Taints, UnreachableTaintTemplate) {
			taintToAdd := *NotReadyTaintTemplate
			if !controllerutil.SwapNodeControllerTaint(ctx, nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{UnreachableTaintTemplate}, node) {
				logger.Error(nil, "Failed to instantly swap UnreachableTaint to NotReadyTaint. Will try again in the next cycle")
			}
		} else if nc.markNodeForTainting(node, v1.ConditionFalse) {
			logger.V(2).Info("Node is NotReady. Adding it to the Taint queue", "node", klog.KObj(node), "timeStamp", decisionTimestamp)
		}
	case v1.ConditionUnknown:
		// 若节点已被打上 NotReady 污点，直接替换为 Unreachable 污点。
		if taintutils.TaintExists(node.Spec.Taints, NotReadyTaintTemplate) {
			taintToAdd := *UnreachableTaintTemplate
			if !controllerutil.SwapNodeControllerTaint(ctx, nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{NotReadyTaintTemplate}, node) {
				logger.Error(nil, "Failed to instantly swap NotReadyTaint to UnreachableTaint. Will try again in the next cycle")
			}
		} else if nc.markNodeForTainting(node, v1.ConditionUnknown) {
			logger.V(2).Info("Node is unresponsive. Adding it to the Taint queue", "node", klog.KObj(node), "timeStamp", decisionTimestamp)
		}
	case v1.ConditionTrue:
		removed, err := nc.markNodeAsReachable(ctx, node)
		if err != nil {
			logger.Error(nil, "Failed to remove taints from node. Will retry in next iteration", "node", klog.KObj(node))
		}
		if removed {
			logger.V(2).Info("Node is healthy again, removed all taints", "node", klog.KObj(node))
		}
	}
}

// labelNodeDisruptionExclusion 是节点上的标签，用于控制该节点是否不参与可用区中断检查。
const labelNodeDisruptionExclusion = "node.kubernetes.io/exclude-disruption"

// isNodeExcludedFromDisruptionChecks 判断节点是否被显式排除在中断检查之外。
func isNodeExcludedFromDisruptionChecks(node *v1.Node) bool {
	if _, ok := node.Labels[labelNodeDisruptionExclusion]; ok {
		return true
	}
	return false
}

// tryUpdateNodeHealth 检查并尝试更新节点条件：返回适用的宽限期、当前与上次观测到的 Ready 条件，以及可能发生的错误。
func (nc *Controller) tryUpdateNodeHealth(ctx context.Context, node *v1.Node) (time.Duration, v1.NodeCondition, *v1.NodeCondition, error) {
	nodeHealth := nc.nodeHealthMap.getDeepCopy(node.Name)
	defer func() {
		nc.nodeHealthMap.set(node.Name, nodeHealth)
	}()

	if utilfeature.DefaultFeatureGate.Enabled(features.NodeControllerLeaseCircuitBreaker) {
		// 这是控制器级别的一致性检查：lease 缓存过期时，任何节点的 lease 都不可信，需等缓存追上。
		err := nc.consistencyStore.EnsureReady(types.NamespacedName{})
		// 缓存未就绪时跳过本轮该节点的处理。
		if err != nil {
			return 0, v1.NodeCondition{}, nil, shortCircuitError{err}
		}
	}

	var gracePeriod time.Duration
	var observedReadyCondition v1.NodeCondition
	_, currentReadyCondition := controllerutil.GetNodeCondition(&node.Status, v1.NodeReady)
	if currentReadyCondition == nil {
		// Ready 条件为空说明 kubelet（或节点控制器）从未上报过节点状态。构造一个虚拟条件，
		// 将心跳/转换时间设为节点创建时间，避免边界情况处理复杂度。
		observedReadyCondition = v1.NodeCondition{
			Type:               v1.NodeReady,
			Status:             v1.ConditionUnknown,
			LastHeartbeatTime:  node.CreationTimestamp,
			LastTransitionTime: node.CreationTimestamp,
		}
		gracePeriod = nc.nodeStartupGracePeriod
		if nodeHealth != nil {
			nodeHealth.status = &node.Status
		} else {
			nodeHealth = &nodeHealthData{
				status:                   &node.Status,
				probeTimestamp:           node.CreationTimestamp,
				readyTransitionTimestamp: node.CreationTimestamp,
			}
		}
	} else {
		// 条件非空时先拷贝，因为后续可能原地修改。
		observedReadyCondition = *currentReadyCondition
		gracePeriod = nc.nodeMonitorGracePeriod
	}
	// 需要处理以下情况：
	// - 新旧状态都没有 Ready 条件：保持原样；
	// - 旧状态无 Ready 而新状态有：控制器重启后 etcd 中已有节点数据；
	// - 旧状态有 Ready 而新状态无：属于异常情况，但仍按新状态补齐；
	// - 两者都有且 LastProbeTime 相同：节点可能无响应，保持原样；
	// - 两者都有、探测时间不同但 Ready 状态相同：仅更新 probeTimestamp；
	// - 两者都有、探测时间与 Ready 状态都不同：状态发生转换，同时更新两个时间戳。
	//   Ready 条件自上次观测以来发生了变化。
	// TODO: things to consider:
	//   - 若 LastProbeTime 出现时间倒流，很可能是异常数据，目前予以忽略；
	//   - 目前唯一由控制器外部触发的正确 Ready 状态转换是 kubelet 将节点标记为 Ready，此处未做检查，
	//     虽然可以这样处理，但看起来并无必要。
	var savedCondition *v1.NodeCondition
	var savedLease *coordv1.Lease
	if nodeHealth != nil {
		_, savedCondition = controllerutil.GetNodeCondition(nodeHealth.status, v1.NodeReady)
		savedLease = nodeHealth.lease
	}
	logger := klog.FromContext(ctx)
	if nodeHealth == nil {
		logger.Info("Missing timestamp for Node. Assuming now as a timestamp", "node", klog.KObj(node))
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition == nil && currentReadyCondition != nil {
		logger.V(1).Info("Creating timestamp entry for newly observed Node", "node", klog.KObj(node))
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition != nil && currentReadyCondition == nil {
		logger.Error(nil, "ReadyCondition was removed from Status of Node", "node", klog.KObj(node))
		// TODO: figure out what to do in this case. For now we do the same thing as above.
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition != nil && currentReadyCondition != nil && savedCondition.LastHeartbeatTime != currentReadyCondition.LastHeartbeatTime {
		var transitionTime metav1.Time
		// ReadyCondition 自上次检查后发生变化时，将转换时间更新为当前时刻。
		if savedCondition.LastTransitionTime != currentReadyCondition.LastTransitionTime {
			logger.V(3).Info("ReadyCondition for Node transitioned from savedCondition to currentReadyCondition", "node", klog.KObj(node), "savedCondition", savedCondition, "currentReadyCondition", currentReadyCondition)
			transitionTime = nc.now()
		} else {
			transitionTime = nodeHealth.readyTransitionTimestamp
		}
		if loggerV := logger.V(5); loggerV.Enabled() {
			loggerV.Info("Node ReadyCondition updated. Updating timestamp", "node", klog.KObj(node), "nodeHealthStatus", nodeHealth.status, "nodeStatus", node.Status)
		} else {
			logger.V(3).Info("Node ReadyCondition updated. Updating timestamp", "node", klog.KObj(node))
		}
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: transitionTime,
		}
	}
	// 只要 lease 续期就更新探测时间。注意：即使 kubelet 从不更新节点状态，
	// 只要持续续租 lease，控制器仍会认为节点健康而不采取行动。
	observedLease, _ := nc.leaseLister.Leases(v1.NamespaceNodeLease).Get(node.Name)
	if observedLease != nil && (savedLease == nil || savedLease.Spec.RenewTime.Before(observedLease.Spec.RenewTime)) {
		nodeHealth.lease = observedLease
		nodeHealth.probeTimestamp = nc.now()
	}

	if nc.now().After(nodeHealth.probeTimestamp.Add(gracePeriod)) {
		if utilfeature.DefaultFeatureGate.Enabled(features.NodeControllerLeaseCircuitBreaker) {
			var nodeHealthLeaseRV string
			if nodeHealth.lease != nil {
				nodeHealthLeaseRV = nodeHealth.lease.ResourceVersion
			}
			// informer 缓存中的 lease 显示已过期，需实时 GET 二次确认，防止因缓存过期误判节点失联。
			liveLease, err := nc.kubeClient.CoordinationV1().Leases(v1.NamespaceNodeLease).Get(ctx, node.Name, metav1.GetOptions{})
			if err == nil {
				if liveLease.ResourceVersion != nodeHealthLeaseRV {
					nc.consistencyStore.WroteAt(
						// 控制器级别的一致性检查：lease 缓存过期时需等其追上后再处理任何节点。
						types.NamespacedName{},
						"", // No specific UID for generic name
						leaseResource,
						liveLease.ResourceVersion,
					)
					return 0, v1.NodeCondition{}, nil, shortCircuitError{&consistencyutil.ConsistencyError{
						ReadRV:        nodeHealthLeaseRV,
						WroteRV:       liveLease.ResourceVersion,
						GroupResource: leaseResource,
					}}
				}
			} else if !apierrors.IsNotFound(err) {
				return 0, v1.NodeCondition{}, nil, shortCircuitError{fmt.Errorf("error looking up lease to verify node %s: %w", node.Name, err)}
			}
		}

		// NodeReady 条件或 lease 的最后更新时间已超过宽限期，
		// 因此无论当前值是什么，都要在控制面把它更新为 Unknown。

		nodeConditionTypes := []v1.NodeConditionType{
			v1.NodeReady,
			v1.NodeMemoryPressure,
			v1.NodeDiskPressure,
			v1.NodePIDPressure,
			// NodeNetworkUnavailable 由控制面组件管理，不在此处置为 Unknown。
			// v1.NodeNetworkUnavailable,
		}

		nowTimestamp := nc.now()
		for _, nodeConditionType := range nodeConditionTypes {
			_, currentCondition := controllerutil.GetNodeCondition(&node.Status, nodeConditionType)
			if currentCondition == nil {
				logger.V(2).Info("Condition of node was never updated by kubelet", "nodeConditionType", nodeConditionType, "node", klog.KObj(node))
				node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
					Type:               nodeConditionType,
					Status:             v1.ConditionUnknown,
					Reason:             "NodeStatusNeverUpdated",
					Message:            "Kubelet never posted node status.",
					LastHeartbeatTime:  node.CreationTimestamp,
					LastTransitionTime: nowTimestamp,
				})
			} else {
				logger.V(2).Info("Node hasn't been updated",
					"node", klog.KObj(node), "duration", nc.now().Time.Sub(nodeHealth.probeTimestamp.Time), "nodeConditionType", nodeConditionType, "currentCondition", currentCondition)
				if currentCondition.Status != v1.ConditionUnknown {
					currentCondition.Status = v1.ConditionUnknown
					currentCondition.Reason = "NodeStatusUnknown"
					currentCondition.Message = "Kubelet stopped posting node status."
					currentCondition.LastTransitionTime = nowTimestamp
				}
			}
		}
		// 条件可能已被修改，重新获取最新的 Ready 条件用于后续比较。
		_, currentReadyCondition = controllerutil.GetNodeCondition(&node.Status, v1.NodeReady)

		if !apiequality.Semantic.DeepEqual(currentReadyCondition, &observedReadyCondition) {
			if _, err := nc.kubeClient.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{}); err != nil {
				logger.Error(err, "Error updating node", "node", klog.KObj(node))
				return gracePeriod, observedReadyCondition, currentReadyCondition, err
			}
			nodeHealth = &nodeHealthData{
				status:                   &node.Status,
				probeTimestamp:           nodeHealth.probeTimestamp,
				readyTransitionTimestamp: nc.now(),
				lease:                    observedLease,
			}
			return gracePeriod, observedReadyCondition, currentReadyCondition, nil
		}
	}

	return gracePeriod, observedReadyCondition, currentReadyCondition, nil
}

// handleDisruption 根据各可用区的节点健康状况切换中断模式，并相应调整驱逐限速器。
func (nc *Controller) handleDisruption(ctx context.Context, zoneToNodeConditions map[string][]*v1.NodeCondition, nodes []*v1.Node) {
	newZoneStates := map[string]ZoneState{}
	allAreFullyDisrupted := true
	logger := klog.FromContext(ctx)
	for k, v := range zoneToNodeConditions {
		zoneSize.WithLabelValues(k).Set(float64(len(v)))
		unhealthy, newState := nc.computeZoneStateFunc(v)
		zoneHealth.WithLabelValues(k).Set(float64(100*(len(v)-unhealthy)) / float64(len(v)))
		unhealthyNodes.WithLabelValues(k).Set(float64(unhealthy))
		if newState != stateFullDisruption {
			allAreFullyDisrupted = false
		}
		newZoneStates[k] = newState
		if _, had := nc.zoneStates[k]; !had {
			logger.Error(nil, "Setting initial state for unseen zone", "zone", k)
			nc.zoneStates[k] = stateInitial
		}
	}

	allWasFullyDisrupted := true
	for k, v := range nc.zoneStates {
		if _, have := zoneToNodeConditions[k]; !have {
			zoneSize.WithLabelValues(k).Set(0)
			zoneHealth.WithLabelValues(k).Set(100)
			unhealthyNodes.WithLabelValues(k).Set(0)
			delete(nc.zoneStates, k)
			continue
		}
		if v != stateFullDisruption {
			allWasFullyDisrupted = false
			break
		}
	}

	// 上一轮或本轮至少有一个节点仍有响应时的处理语义：
	// - 新状态为 partialDisruption：调用用户自定义函数计算新的限速值；
	// - 新状态为 normal：恢复正常驱逐速率；
	// - 新状态为 fullDisruption：恢复常规驱逐速率，但若集群所有可用区均完全中断，则停止全部驱逐。
	if !allAreFullyDisrupted || !allWasFullyDisrupted {
		// 进入全局完全中断模式。
		if allAreFullyDisrupted {
			logger.Info("Controller detected that all Nodes are not-Ready. Entering master disruption mode")
			for i := range nodes {
				_, err := nc.markNodeAsReachable(ctx, nodes[i])
				if err != nil {
					logger.Error(nil, "Failed to remove taints from Node", "node", klog.KObj(nodes[i]))
				}
			}
			// 所有可用区都不健康时停止全部驱逐，避免雪崩。
			for k := range nc.zoneStates {
				nc.zoneNoExecuteTainter[k].SwapLimiter(0)
			}
			for k := range nc.zoneStates {
				nc.zoneStates[k] = stateFullDisruption
			}
			// 限速器已全部更新，可提前返回。
			return
		}
		// 退出全局完全中断模式。
		if allWasFullyDisrupted {
			logger.Info("Controller detected that some Nodes are Ready. Exiting master disruption mode")
			// 退出中断模式时刷新所有节点的探测时间，给节点重新上报健康状态的宽限期。
			now := nc.now()
			for i := range nodes {
				v := nc.nodeHealthMap.getDeepCopy(nodes[i].Name)
				v.probeTimestamp = now
				v.readyTransitionTimestamp = now
				nc.nodeHealthMap.set(nodes[i].Name, v)
			}
			// 按新状态重置所有可用区的限速器。
			for k := range nc.zoneStates {
				nc.setLimiterInZone(k, len(zoneToNodeConditions[k]), newZoneStates[k])
				nc.zoneStates[k] = newZoneStates[k]
			}
			return
		}
		// 此时至少有一个可用区未完全中断，按各可用区状态分别应用默认限速策略。
		// 此时可以直接使用限速器的默认行为。
		for k, v := range nc.zoneStates {
			newState := newZoneStates[k]
			if v == newState {
				continue
			}
			logger.Info("Controller detected that zone is now in new state", "zone", k, "newState", newState)
			nc.setLimiterInZone(k, len(zoneToNodeConditions[k]), newState)
			nc.zoneStates[k] = newState
		}
	}
}

// podUpdated 在 Pod 被调度到节点或节点发生变化时将其加入 podUpdateQueue。
func (nc *Controller) podUpdated(oldPod, newPod *v1.Pod) {
	if newPod == nil {
		return
	}
	if len(newPod.Spec.NodeName) != 0 && (oldPod == nil || newPod.Spec.NodeName != oldPod.Spec.NodeName) {
		podItem := podUpdateItem{newPod.Namespace, newPod.Name}
		nc.podUpdateQueue.Add(podItem)
	}
}

// doPodProcessingWorker 消费 podUpdateQueue 并处理每个 Pod 的调度后状态。
func (nc *Controller) doPodProcessingWorker(ctx context.Context) {
	for {
		obj, shutdown := nc.podUpdateQueue.Get()
		// 队列会随 ctx 结束而关闭，通过 shutdown 标志即可退出。
		if shutdown {
			return
		}

		podItem := obj
		nc.processPod(ctx, podItem)
	}
}

// processPod 处理 Pod 被调度到节点后的状态更新：
// 1. 节点 Ready=true 时取消该 Pod 的污点驱逐；
// 2. 节点 Ready=false 或 Unknown 时执行驱逐并将 Pod 标记为 NotReady；
// 3. 节点不在缓存中时跳过。
func (nc *Controller) processPod(ctx context.Context, podItem podUpdateItem) {
	defer nc.podUpdateQueue.Done(podItem)
	pod, err := nc.podLister.Pods(podItem.namespace).Get(podItem.name)
	logger := klog.FromContext(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Pod 已被删除，无需重试。
			return
		}
		logger.Info("Failed to read pod", "pod", klog.KRef(podItem.namespace, podItem.name), "err", err)
		nc.podUpdateQueue.AddRateLimited(podItem)
		return
	}

	nodeName := pod.Spec.NodeName

	nodeHealth := nc.nodeHealthMap.getDeepCopy(nodeName)
	if nodeHealth == nil {
		// 尚未采集到该节点数据，或节点已被移除，跳过。
		return
	}

	_, err = nc.nodeLister.Get(nodeName)
	if err != nil {
		logger.Info("Failed to read node", "node", klog.KRef("", nodeName), "err", err)
		nc.podUpdateQueue.AddRateLimited(podItem)
		return
	}

	_, currentReadyCondition := controllerutil.GetNodeCondition(nodeHealth.status, v1.NodeReady)
	if currentReadyCondition == nil {
		// 缺失 Ready 条件只可能发生在节点刚加入（或条件被恶意删除）时；
		// 两种情况都会在下次节点更新事件中被正确处理。
		return
	}

	pods := []*v1.Pod{pod}
	if currentReadyCondition.Status != v1.ConditionTrue {
		if err := controllerutil.MarkPodsNotReady(ctx, nc.kubeClient, nc.recorder, pods, nodeName); err != nil {
			logger.Info("Unable to mark pod NotReady on node", "pod", klog.KRef(podItem.namespace, podItem.name), "node", klog.KRef("", nodeName), "err", err)
			nc.podUpdateQueue.AddRateLimited(podItem)
		}
	}
}

// setLimiterInZone 按可用区状态切换 NoExecute 污点队列的限速值。
func (nc *Controller) setLimiterInZone(zone string, zoneSize int, state ZoneState) {
	switch state {
	case stateNormal:
		nc.zoneNoExecuteTainter[zone].SwapLimiter(nc.evictionLimiterQPS)
	case statePartialDisruption:
		nc.zoneNoExecuteTainter[zone].SwapLimiter(
			nc.enterPartialDisruptionFunc(zoneSize))
	case stateFullDisruption:
		nc.zoneNoExecuteTainter[zone].SwapLimiter(
			nc.enterFullDisruptionFunc(zoneSize))
	}
}

// classifyNodes 将节点分为三类：
//  1. added：出现在 allNodes 但不在 knownNodeSet 中的新节点；
//  2. deleted：在 knownNodeSet 中但已从 allNodes 消失的节点；
//  3. newZoneRepresentatives：同时存在于 allNodes 与 knownNodeSet、但其所属可用区尚无状态记录的代表节点。
func (nc *Controller) classifyNodes(allNodes []*v1.Node) (added, deleted, newZoneRepresentatives []*v1.Node) {
	for i := range allNodes {
		if _, has := nc.knownNodeSet[allNodes[i].Name]; !has {
			added = append(added, allNodes[i])
		} else {
			// 目前只把「所属可用区首次出现」视为需要处理的更新。
			zone := nodetopology.GetZoneKey(allNodes[i])
			if _, found := nc.zoneStates[zone]; !found {
				newZoneRepresentatives = append(newZoneRepresentatives, allNodes[i])
			}
		}
	}

	// 已知节点数与观测节点数不一致时，说明有节点被移除，需要差分找出具体哪些节点。
	if len(nc.knownNodeSet)+len(added) != len(allNodes) {
		knowSetCopy := map[string]*v1.Node{}
		for k, v := range nc.knownNodeSet {
			knowSetCopy[k] = v
		}
		for i := range allNodes {
			delete(knowSetCopy, allNodes[i].Name)
		}
		for i := range knowSetCopy {
			deleted = append(deleted, knowSetCopy[i])
		}
	}
	return
}

// HealthyQPSFunc 返回集群正常状态下的驱逐速率；nodeNum 参数仅为与 ReducedQPSFunc 保持一致。
func (nc *Controller) HealthyQPSFunc(nodeNum int) float32 {
	return nc.evictionLimiterQPS
}

// ReducedQPSFunc 返回部分中断时的驱逐速率：大集群降速驱逐，小集群则完全停止驱逐。
func (nc *Controller) ReducedQPSFunc(nodeNum int) float32 {
	if int32(nodeNum) > nc.largeClusterThreshold {
		return nc.secondaryEvictionLimiterQPS
	}
	return 0
}

// addPodEvictorForNewZone 在发现新可用区时创建对应的限速队列并初始化指标。
func (nc *Controller) addPodEvictorForNewZone(logger klog.Logger, node *v1.Node) {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	zone := nodetopology.GetZoneKey(node)
	if _, found := nc.zoneStates[zone]; !found {
		nc.zoneStates[zone] = stateInitial
		nc.zoneNoExecuteTainter[zone] =
			scheduler.NewRateLimitedTimedQueue(
				flowcontrol.NewTokenBucketRateLimiter(nc.evictionLimiterQPS, scheduler.EvictionRateLimiterBurst))
		// 初始化新可用区的指标，避免 Prometheus 查询时出现空序列。
		logger.Info("Initializing eviction metric for zone", "zone", zone)
		evictionsTotal.WithLabelValues(zone).Add(0)
	}
}

// markNodeForTainting 将节点加入所属可用区的限速污点队列，并在状态切换时清理互斥污点标记。
func (nc *Controller) markNodeForTainting(node *v1.Node, status v1.ConditionStatus) bool {
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()
	if status == v1.ConditionFalse {
		if !taintutils.TaintExists(node.Spec.Taints, NotReadyTaintTemplate) {
			nc.zoneNoExecuteTainter[nodetopology.GetZoneKey(node)].Remove(node.Name)
		}
	}

	if status == v1.ConditionUnknown {
		if !taintutils.TaintExists(node.Spec.Taints, UnreachableTaintTemplate) {
			nc.zoneNoExecuteTainter[nodetopology.GetZoneKey(node)].Remove(node.Name)
		}
	}

	return nc.zoneNoExecuteTainter[nodetopology.GetZoneKey(node)].Add(node.Name, string(node.UID))
}

// markNodeAsReachable 移除节点的 Unreachable/NotReady 污点，并从限速队列删除，返回是否确有移除发生。
func (nc *Controller) markNodeAsReachable(ctx context.Context, node *v1.Node) (bool, error) {
	err := controller.RemoveTaintOffNode(ctx, nc.kubeClient, node.Name, node, UnreachableTaintTemplate)
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Failed to remove unreachable taint from node", "node", klog.KObj(node))
		return false, err
	}
	err = controller.RemoveTaintOffNode(ctx, nc.kubeClient, node.Name, node, NotReadyTaintTemplate)
	if err != nil {
		logger.Error(err, "Failed to remove not-ready taint from node", "node", klog.KObj(node))
		return false, err
	}
	nc.evictorLock.Lock()
	defer nc.evictorLock.Unlock()

	return nc.zoneNoExecuteTainter[nodetopology.GetZoneKey(node)].Remove(node.Name), nil
}

// ComputeZoneState 根据节点 Ready 条件计算可用区状态，返回未就绪节点数与状态：
// - 没有 Ready 节点时为 fullyDisrupted；
// - 未就绪节点占比达到 unhealthyZoneThreshold 时为 partiallyDisrupted；
// - 其他情况为 normal。
func (nc *Controller) ComputeZoneState(nodeReadyConditions []*v1.NodeCondition) (int, ZoneState) {
	readyNodes := 0
	notReadyNodes := 0
	for i := range nodeReadyConditions {
		if nodeReadyConditions[i] != nil && nodeReadyConditions[i].Status == v1.ConditionTrue {
			readyNodes++
		} else {
			notReadyNodes++
		}
	}
	switch {
	case readyNodes == 0 && notReadyNodes > 0:
		return notReadyNodes, stateFullDisruption
	case notReadyNodes > 2 && float32(notReadyNodes)/float32(notReadyNodes+readyNodes) >= nc.unhealthyZoneThreshold:
		return notReadyNodes, statePartialDisruption
	default:
		return notReadyNodes, stateNormal
	}
}

// reconcileNodeLabels 以稳定版标签为准调和 beta 版标签，并按需补齐缺失的辅助标签。
func (nc *Controller) reconcileNodeLabels(ctx context.Context, nodeName string) error {
	node, err := nc.nodeLister.Get(nodeName)
	if err != nil {
		// 节点已被删除，忽略。
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if node.Labels == nil {
		// 节点没有任何标签，无需调和。
		return nil
	}

	labelsToUpdate := map[string]string{}
	for _, r := range labelReconcileInfo {
		primaryValue, primaryExists := node.Labels[r.primaryKey]
		secondaryValue, secondaryExists := node.Labels[r.secondaryKey]

		if !primaryExists {
			// 主标签不存在：在支持的版本偏差范围内且无外部组件修改节点对象时不应发生，忽略。
			continue
		}
		if secondaryExists && primaryValue != secondaryValue {
			// 辅助标签存在但与主标签值不一致，需要修正。
			labelsToUpdate[r.secondaryKey] = primaryValue

		} else if !secondaryExists && r.ensureSecondaryExists {
			// 按主标签值补齐辅助标签。
			labelsToUpdate[r.secondaryKey] = primaryValue
		}
	}

	if len(labelsToUpdate) == 0 {
		return nil
	}
	if !controllerutil.AddOrUpdateLabelsOnNode(ctx, nc.kubeClient, labelsToUpdate, node) {
		return fmt.Errorf("failed update labels for node %+v", node)
	}
	return nil
}
