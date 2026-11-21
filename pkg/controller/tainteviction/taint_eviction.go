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

package tainteviction

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	corev1informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/controller/tainteviction/metrics"
	controllerutil "k8s.io/kubernetes/pkg/controller/util/node"
	utilpod "k8s.io/kubernetes/pkg/util/pod"
)

const (
	// TODO (k82cn): 确定合理的 worker/channel 数量，并将其向上传递，
	// 使之成为 Run() 函数的一个参数。

	// NodeUpdateChannelSize 定义节点更新事件通道的缓冲区大小。
	NodeUpdateChannelSize = 10
	// UpdateWorkerSize 定义处理节点更新和/或 Pod 更新的 worker 数量。
	UpdateWorkerSize = 8
	// podUpdateChannelSize 定义 Pod 更新事件通道的缓冲区大小；设为 1 可让 worker 及时取走事件处理。
	podUpdateChannelSize = 1
	// retries 定义删除 Pod 失败后的最大重试次数。
	retries = 5
)

// nodeUpdateItem 表示一个待处理的节点更新事件，只携带节点名。
type nodeUpdateItem struct {
	nodeName string
}

// podUpdateItem 表示一个待处理的 Pod 更新事件，记录 Pod 的命名空间、名称及其所在节点。
type podUpdateItem struct {
	podName      string
	podNamespace string
	nodeName     string
}

// hash 将字符串稳定映射到 [0, max) 的一个分片编号。
// 用确定性哈希（而非随机分发）保证同一节点的所有事件始终落到同一个 worker，
// 从而避免同一节点的状态在多个 worker 之间产生竞态。
func hash(val string, max int) int {
	hasher := fnv.New32a()
	io.WriteString(hasher, val)
	return int(hasher.Sum32() % uint32(max))
}

// GetPodsByNodeNameFunc 返回被调度到指定节点上的 Pod 列表。
type GetPodsByNodeNameFunc func(nodeName string) ([]*v1.Pod, error)

// Controller 监听 Taint/Toleration 的变化，负责驱逐带有 NoExecute 污点节点上的 Pod。
type Controller struct {
	// name 为本控制器名称，用于日志和事件来源标识。
	name string

	// client 用于与 API Server 通信。
	client clientset.Interface
	// broadcaster 负责把事件分发到日志和 API Server 的事件接收端。
	broadcaster record.EventBroadcaster
	// recorder 用于记录 Pod 删除与取消删除事件。
	recorder record.EventRecorder
	// podLister 提供 Pod 的本地缓存读取能力。
	podLister corelisters.PodLister
	// podListerSynced 用于等待 Pod 本地缓存完成同步。
	podListerSynced cache.InformerSynced
	// nodeLister 提供 Node 的本地缓存读取能力。
	nodeLister corelisters.NodeLister
	// nodeListerSynced 用于等待 Node 本地缓存完成同步。
	nodeListerSynced cache.InformerSynced
	// getPodsAssignedToNode 查询指定节点上已经分配的所有 Pod。
	getPodsAssignedToNode GetPodsByNodeNameFunc

	// taintEvictionQueue 是延迟驱逐队列，承载到达容忍时间后执行的删除工作。
	taintEvictionQueue *TimedWorkerQueue
	// 缓存节点名到该节点上所有 NoExecute 污点的映射；读取和写入必须持有下面的锁。
	taintedNodesLock sync.Mutex
	taintedNodes     map[string][]v1.Taint

	// 两类更新通道都按节点哈希分片，保证同一节点的节点与 Pod 事件由同一 worker 顺序处理。
	nodeUpdateChannels []chan nodeUpdateItem
	// podUpdateChannels 是按节点分片的 Pod 更新事件通道。
	podUpdateChannels []chan podUpdateItem

	// 两个工作队列负责去重，事件交给通道后由对应 worker 调用 Done 标记完成。
	nodeUpdateQueue workqueue.TypedInterface[nodeUpdateItem]
	// podUpdateQueue 保存待处理的 Pod 更新事件。
	podUpdateQueue workqueue.TypedInterface[podUpdateItem]
}

// deletePodHandler 构造延迟队列到期后执行的删除回调。
// 回调先发出删除事件，再重试给 Pod 打上 DisruptionTarget 条件并删除，
// 重试是为了容忍 API Server 的短暂冲突或失败。
func deletePodHandler(c clientset.Interface, emitEventFunc func(types.NamespacedName), controllerName string) func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
	return func(ctx context.Context, fireAt time.Time, args *WorkArgs) error {
		ns := args.Object.Namespace
		name := args.Object.Name
		klog.FromContext(ctx).Info("Deleting pod", "controller", controllerName, "pod", args.Object)
		if emitEventFunc != nil {
			emitEventFunc(args.Object.NamespacedName)
		}
		var err error
		for i := 0; i < retries; i++ {
			// 只有全部重试失败才返回错误，避免偶发失败导致驱逐丢失。
			err = addConditionAndDeletePod(ctx, c, name, ns)
			if err == nil {
				metrics.PodDeletionsTotal.Inc()
				metrics.PodDeletionsLatency.Observe(float64(time.Since(fireAt) * time.Second))
				break
			}
			// 短暂退避，给 API Server 冲突或网络抖动留出恢复时间。
			time.Sleep(10 * time.Millisecond)
		}
		return err
	}
}

// addConditionAndDeletePod 给 Pod 打上 DisruptionTarget 状态条件后将其删除。
// 先记录条件再删除，是为了让监控与用户能区分"被驱逐"和普通删除。
func addConditionAndDeletePod(ctx context.Context, c clientset.Interface, name, ns string) (err error) {
	pod, err := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	// 在副本上更新条件，避免修改 informer 缓存中的对象。
	newStatus := pod.Status.DeepCopy()
	updated := apipod.UpdatePodCondition(newStatus, &v1.PodCondition{
		Type:               v1.DisruptionTarget,
		ObservedGeneration: apipod.CalculatePodConditionObservedGeneration(&pod.Status, pod.Generation, v1.DisruptionTarget),
		Status:             v1.ConditionTrue,
		Reason:             "DeletionByTaintManager",
		Message:            "Taint manager: deleting due to NoExecute taint",
	})
	// 条件已存在时无需重复打补丁，直接删除即可。
	if updated {
		if _, _, _, err := utilpod.PatchPodStatus(ctx, c, pod.Namespace, pod.Name, pod.UID, pod.Status, *newStatus); err != nil {
			return err
		}
	}
	return c.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
}

// getNoExecuteTaints 从污点列表中筛选出 NoExecute 类型的污点。
// 只有 NoExecute 污点会触发驱逐，其他效果与本控制器无关。
func getNoExecuteTaints(taints []v1.Taint) []v1.Taint {
	result := []v1.Taint{}
	for i := range taints {
		if taints[i].Effect == v1.TaintEffectNoExecute {
			result = append(result, taints[i])
		}
	}
	return result
}

// getMinTolerationTime 返回给定容忍列表中最短的容忍时间，无限容忍返回 -1。
func getMinTolerationTime(tolerations []v1.Toleration) time.Duration {
	minTolerationTime := int64(math.MaxInt64)
	// 没有任何容忍意味着完全不容忍，应立即驱逐，故返回 0。
	if len(tolerations) == 0 {
		return 0
	}

	for i := range tolerations {
		// 未指定 TolerationSeconds 的容忍是永久有效的，跳过即可。
		if tolerations[i].TolerationSeconds != nil {
			tolerationSeconds := *(tolerations[i].TolerationSeconds)
			// 非正数表示永久容忍，直接返回 0 让 Pod 立即被驱逐。
			if tolerationSeconds <= 0 {
				return 0
			} else if tolerationSeconds < minTolerationTime {
				minTolerationTime = tolerationSeconds
			}
		}
	}

	// 所有容忍都没有时限，等价于永久容忍，用 -1 表示。
	if minTolerationTime == int64(math.MaxInt64) {
		return -1
	}
	return time.Duration(minTolerationTime) * time.Second
}

// New 创建一个新的 Controller，使用传入的 clientset 与 API Server 通信。
func New(ctx context.Context, c clientset.Interface, podInformer corev1informers.PodInformer, nodeInformer corev1informers.NodeInformer, controllerName string) (*Controller, error) {
	logger := klog.FromContext(ctx)
	// 先注册指标，保证后续使用指标时名称已就绪。
	metrics.Register()
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: controllerName})

	podIndexer := podInformer.Informer().GetIndexer()

	tm := &Controller{
		name: controllerName,

		client:           c,
		broadcaster:      eventBroadcaster,
		recorder:         recorder,
		podLister:        podInformer.Lister(),
		podListerSynced:  podInformer.Informer().HasSynced,
		nodeLister:       nodeInformer.Lister(),
		nodeListerSynced: nodeInformer.Informer().HasSynced,
		getPodsAssignedToNode: func(nodeName string) ([]*v1.Pod, error) {
			objs, err := podIndexer.ByIndex("spec.nodeName", nodeName)
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
		},
		taintedNodes: make(map[string][]v1.Taint),

		nodeUpdateQueue: workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[nodeUpdateItem]{Name: "noexec_taint_node"}),
		podUpdateQueue:  workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[podUpdateItem]{Name: "noexec_taint_pod"}),
	}
	tm.taintEvictionQueue = CreateWorkerQueue(deletePodHandler(c, tm.emitPodDeletionEvent, tm.name))

	_, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			tm.PodUpdated(nil, pod)
		},
		UpdateFunc: func(prev, obj interface{}) {
			prevPod := prev.(*v1.Pod)
			newPod := obj.(*v1.Pod)
			tm.PodUpdated(prevPod, newPod)
		},
		DeleteFunc: func(obj interface{}) {
			pod, isPod := obj.(*v1.Pod)
			// 删除事件可能携带 DeletedFinalStateUnknown 包装而非原始 Pod，
			// 需要先解包，否则会把已删除的 Pod 当作普通对象而漏掉处理。
			if !isPod {
				deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					logger.Error(nil, "Received unexpected object", "object", obj)
					return
				}
				pod, ok = deletedState.Obj.(*v1.Pod)
				if !ok {
					logger.Error(nil, "DeletedFinalStateUnknown contained non-Pod object", "object", deletedState.Obj)
					return
				}
			}
			tm.PodUpdated(pod, nil)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to add pod event handler: %w", err)
	}

	_, err = nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controllerutil.CreateAddNodeHandler(func(node *v1.Node) error {
			tm.NodeUpdated(nil, node)
			return nil
		}),
		UpdateFunc: controllerutil.CreateUpdateNodeHandler(func(oldNode, newNode *v1.Node) error {
			tm.NodeUpdated(oldNode, newNode)
			return nil
		}),
		DeleteFunc: controllerutil.CreateDeleteNodeHandler(logger, func(node *v1.Node) error {
			tm.NodeUpdated(node, nil)
			return nil
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to add node event handler: %w", err)
	}

	return tm, nil
}

// Run 启动控制器并持续运行，直到 ctx 被取消。
func (tc *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting", "controller", tc.name)

	// 启动事件处理流水线，将事件同时写入日志和 API Server。
	tc.broadcaster.StartStructuredLogging(3)
	tc.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: tc.client.CoreV1().Events("")})
	logger.Info("Sending events to API server")
	defer tc.broadcaster.Shutdown()

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down controller", "controller", tc.name)
		tc.nodeUpdateQueue.ShutDown()
		tc.podUpdateQueue.ShutDown()
		tc.taintEvictionQueue.CancelAndWait()
		wg.Wait()
	}()

	// 先等待缓存同步，否则 lister 读到的数据不完整，可能做出错误的驱逐决策。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, tc.podListerSynced, tc.nodeListerSynced) {
		return
	}

	for i := 0; i < UpdateWorkerSize; i++ {
		tc.nodeUpdateChannels = append(tc.nodeUpdateChannels, make(chan nodeUpdateItem, NodeUpdateChannelSize))
		tc.podUpdateChannels = append(tc.podUpdateChannels, make(chan podUpdateItem, podUpdateChannelSize))
	}

	// 下面两个 goroutine 负责把工作项从队列取出并投递到按节点分片的通道。
	wg.Go(func() {
		for {
			nodeUpdate, shutdown := tc.nodeUpdateQueue.Get()
			if shutdown {
				break
			}
			hash := hash(nodeUpdate.nodeName, UpdateWorkerSize)
			select {
			case <-ctx.Done():
				tc.nodeUpdateQueue.Done(nodeUpdate)
				return
			case tc.nodeUpdateChannels[hash] <- nodeUpdate:
				// Done 由消费该通道的 worker 调用，确保事件处理完才从队列移除。
			}
		}
	})

	wg.Go(func() {
		for {
			podUpdate, shutdown := tc.podUpdateQueue.Get()
			if shutdown {
				break
			}
			// Pod 与节点事件必须由同一 worker 处理，以确保写入 tc.taintedNodes
			// 与读取该状态决定驱逐之间不会出现竞态；没有这一假设代码也许可行，但无法保证。
			hash := hash(podUpdate.nodeName, UpdateWorkerSize)
			select {
			case <-ctx.Done():
				tc.podUpdateQueue.Done(podUpdate)
				return
			case tc.podUpdateChannels[hash] <- podUpdate:
				// Done 由消费该通道的 worker 调用，确保事件处理完才从队列移除。
			}
		}
	})

	for i := 0; i < UpdateWorkerSize; i++ {
		wg.Go(func() {
			tc.worker(ctx, i)
		})
	}
	<-ctx.Done()
}

// worker 持续处理分配给自己的节点与 Pod 更新事件。
// 节点更新优先于 Pod 更新，因为节点污点的变化必须尽快生效，
// 否则用户或系统会等到 Pod 队列排空后才能开始驱逐污点节点上的 Pod。
func (tc *Controller) worker(ctx context.Context, worker int) {
	for {
		select {
		case <-ctx.Done():
			return
		case nodeUpdate := <-tc.nodeUpdateChannels[worker]:
			tc.handleNodeUpdate(ctx, nodeUpdate)
			tc.nodeUpdateQueue.Done(nodeUpdate)
		case podUpdate := <-tc.podUpdateChannels[worker]:
			// 处理 Pod 更新前先清空节点队列，确保最新的污点状态已生效。
		priority:
			for {
				select {
				case nodeUpdate := <-tc.nodeUpdateChannels[worker]:
					tc.handleNodeUpdate(ctx, nodeUpdate)
					tc.nodeUpdateQueue.Done(nodeUpdate)
				default:
					break priority
				}
			}
			// 节点队列清空后再按最新状态处理 Pod 更新。
			tc.handlePodUpdate(ctx, podUpdate)
			tc.podUpdateQueue.Done(podUpdate)
		}
	}
}

// PodUpdated 通知 NoExecuteTaintManager 有 Pod 发生变化。
// 仅在容忍配置或所在节点发生变化时才入队，其余变化与本控制器无关。
func (tc *Controller) PodUpdated(oldPod *v1.Pod, newPod *v1.Pod) {
	podName := ""
	podNamespace := ""
	nodeName := ""
	oldTolerations := []v1.Toleration{}
	if oldPod != nil {
		podName = oldPod.Name
		podNamespace = oldPod.Namespace
		nodeName = oldPod.Spec.NodeName
		oldTolerations = oldPod.Spec.Tolerations
	}
	newTolerations := []v1.Toleration{}
	if newPod != nil {
		podName = newPod.Name
		podNamespace = newPod.Namespace
		nodeName = newPod.Spec.NodeName
		newTolerations = newPod.Spec.Tolerations
	}

	// 容忍与调度节点都没变，驱逐决策不会改变，直接忽略该事件。
	if oldPod != nil && newPod != nil && helper.Semantic.DeepEqual(oldTolerations, newTolerations) && oldPod.Spec.NodeName == newPod.Spec.NodeName {
		return
	}
	updateItem := podUpdateItem{
		podName:      podName,
		podNamespace: podNamespace,
		nodeName:     nodeName,
	}

	tc.podUpdateQueue.Add(updateItem)
}

// NodeUpdated 通知 NoExecuteTaintManager 有节点发生变化。
// 只有 NoExecute 污点集合发生变化才需要重新评估驱逐决策。
func (tc *Controller) NodeUpdated(oldNode *v1.Node, newNode *v1.Node) {
	nodeName := ""
	oldTaints := []v1.Taint{}
	if oldNode != nil {
		nodeName = oldNode.Name
		oldTaints = getNoExecuteTaints(oldNode.Spec.Taints)
	}

	newTaints := []v1.Taint{}
	if newNode != nil {
		nodeName = newNode.Name
		newTaints = getNoExecuteTaints(newNode.Spec.Taints)
	}

	// NoExecute 污点未变，驱逐决策也不会变，无需入队。
	if oldNode != nil && newNode != nil && helper.Semantic.DeepEqual(oldTaints, newTaints) {
		return
	}
	updateItem := nodeUpdateItem{
		nodeName: nodeName,
	}

	tc.nodeUpdateQueue.Add(updateItem)
}

// cancelWorkWithEvent 取消对该 Pod 已排期的驱逐，并在确实取消了工作时发出取消事件。
func (tc *Controller) cancelWorkWithEvent(logger klog.Logger, nsName types.NamespacedName) {
	// 只有原本存在待执行的工作才发事件，避免产生大量无意义的事件。
	if tc.taintEvictionQueue.CancelWork(logger, nsName.String()) {
		tc.emitCancelPodDeletionEvent(nsName)
	}
}

// processPodOnNode 根据节点污点与 Pod 容忍重新计算驱逐时间：
// 不容忍则立即驱逐，永久容忍则取消驱逐，有限容忍则按最早的可容忍期限排期。
func (tc *Controller) processPodOnNode(
	ctx context.Context,
	podNamespacedName types.NamespacedName,
	nodeName string,
	tolerations []v1.Toleration,
	taints []v1.Taint,
	now time.Time,
) {
	logger := klog.FromContext(ctx)
	// 节点已无污点，之前排期的驱逐必须撤销。
	if len(taints) == 0 {
		tc.cancelWorkWithEvent(logger, podNamespacedName)
	}
	allTolerated, usedTolerations := v1helper.GetMatchingTolerations(logger, taints, tolerations)
	if !allTolerated {
		logger.V(2).Info("Not all taints are tolerated after update for pod on node", "pod", podNamespacedName.String(), "node", klog.KRef("", nodeName))
		// 马上就要删除 Pod，先取消已排期的驱逐，避免重复执行。
		tc.cancelWorkWithEvent(logger, podNamespacedName)
		tc.taintEvictionQueue.AddWork(ctx, NewWorkArgs(podNamespacedName.Name, podNamespacedName.Namespace), now, now)
		return
	}
	minTolerationTime := getMinTolerationTime(usedTolerations)
	// getMinTolerationTime 用负值表示永久容忍，此时不会再触发驱逐。
	if minTolerationTime < 0 {
		logger.V(4).Info("Current tolerations for pod tolerate forever, cancelling any scheduled deletion", "pod", podNamespacedName.String())
		tc.cancelWorkWithEvent(logger, podNamespacedName)
		return
	}

	startTime := now
	triggerTime := startTime.Add(minTolerationTime)
	scheduledEviction := tc.taintEvictionQueue.GetWorkerUnsafe(podNamespacedName.String())
	if scheduledEviction != nil {
		// 沿用原排期的起始时间，避免每次状态刷新都把容忍期限重新计时。
		startTime = scheduledEviction.CreatedAt
		// 已排期的时间更早，说明现有安排更激进，无需更新。
		if startTime.Add(minTolerationTime).Before(triggerTime) {
			return
		}
		// 新计算出的时间更晚，先撤销旧工作再按新时间排期。
		tc.cancelWorkWithEvent(logger, podNamespacedName)
	}
	tc.taintEvictionQueue.AddWork(ctx, NewWorkArgs(podNamespacedName.Name, podNamespacedName.Namespace), startTime, triggerTime)
}

// handlePodUpdate 处理 Pod 更新：删除事件取消排期，创建或更新事件按最新状态重新评估驱逐。
func (tc *Controller) handlePodUpdate(ctx context.Context, podUpdate podUpdateItem) {
	pod, err := tc.podLister.Pods(podUpdate.podNamespace).Get(podUpdate.podName)
	logger := klog.FromContext(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Pod 已删除，无需再驱逐，取消可能存在的排期。
			podNamespacedName := types.NamespacedName{Namespace: podUpdate.podNamespace, Name: podUpdate.podName}
			logger.V(4).Info("Noticed pod deletion", "pod", podNamespacedName)
			tc.cancelWorkWithEvent(logger, podNamespacedName)
			return
		}
		utilruntime.HandleError(fmt.Errorf("could not get pod %s/%s: %v", podUpdate.podName, podUpdate.podNamespace, err))
		return
	}

	// 队列按节点名分片，事件所在分片可能与 Pod 当前节点不一致；
	// 只有负责当前节点状态的 worker 才能处理该 Pod，避免基于过期节点做出决策。
	if pod.Spec.NodeName != podUpdate.nodeName {
		return
	}

	// 创建或更新：先确认 Pod 已被调度，未调度的 Pod 不受节点污点影响。
	podNamespacedName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	logger.V(4).Info("Noticed pod update", "pod", podNamespacedName)
	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return
	}
	taints, ok := func() ([]v1.Taint, bool) {
		tc.taintedNodesLock.Lock()
		defer tc.taintedNodesLock.Unlock()
		taints, ok := tc.taintedNodes[nodeName]
		return taints, ok
	}()
	// 节点可能已被删除或污点已被清除，对应的驱逐取消逻辑已在节点更新时执行，
	// 这里找不到污点记录就直接返回。
	if !ok {
		return
	}
	tc.processPodOnNode(ctx, podNamespacedName, nodeName, pod.Spec.Tolerations, taints, time.Now())
}

// handleNodeUpdate 处理节点更新：维护 NoExecute 污点缓存，并按最新污点重新评估其上所有 Pod 的驱逐。
func (tc *Controller) handleNodeUpdate(ctx context.Context, nodeUpdate nodeUpdateItem) {
	node, err := tc.nodeLister.Get(nodeUpdate.nodeName)
	logger := klog.FromContext(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// 节点已删除，清除其污点缓存，防止后续误判。
			logger.V(4).Info("Noticed node deletion", "node", klog.KRef("", nodeUpdate.nodeName))
			tc.taintedNodesLock.Lock()
			defer tc.taintedNodesLock.Unlock()
			delete(tc.taintedNodes, nodeUpdate.nodeName)
			return
		}
		utilruntime.HandleError(fmt.Errorf("cannot get node %s: %v", nodeUpdate.nodeName, err))
		return
	}

	// 创建或更新：先把最新污点写入缓存，再处理节点上的 Pod。
	logger.V(4).Info("Noticed node update", "node", klog.KObj(node))
	taints := getNoExecuteTaints(node.Spec.Taints)
	func() {
		tc.taintedNodesLock.Lock()
		defer tc.taintedNodesLock.Unlock()
		logger.V(4).Info("Updating known taints on node", "node", klog.KObj(node), "taints", taints)
		if len(taints) == 0 {
			delete(tc.taintedNodes, node.Name)
		} else {
			tc.taintedNodes[node.Name] = taints
		}
	}()

	// 必须先更新 tc.taintedNodes 再查询节点上的 Pod：
	// 即使查询有延迟，后续 Pod 更新也会通过 PodUpdated 读取最新缓存来补救遗漏的驱逐。
	pods, err := tc.getPodsAssignedToNode(node.Name)
	if err != nil {
		logger.Error(err, "Failed to get pods assigned to node", "node", klog.KObj(node))
		return
	}
	if len(pods) == 0 {
		return
	}
	// 节点已无污点，直接取消其上所有 Pod 的驱逐，省去逐个评估。
	if len(taints) == 0 {
		logger.V(4).Info("All taints were removed from the node. Cancelling all evictions...", "node", klog.KObj(node))
		for i := range pods {
			tc.cancelWorkWithEvent(logger, types.NamespacedName{Namespace: pods[i].Namespace, Name: pods[i].Name})
		}
		return
	}

	now := time.Now()
	for _, pod := range pods {
		podNamespacedName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		tc.processPodOnNode(ctx, podNamespacedName, node.Name, pod.Spec.Tolerations, taints, now)
	}
}

// emitPodDeletionEvent 记录 Pod 被标记为删除的事件；未配置 recorder 时静默跳过。
func (tc *Controller) emitPodDeletionEvent(nsName types.NamespacedName) {
	if tc.recorder == nil {
		return
	}
	ref := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       nsName.Name,
		Namespace:  nsName.Namespace,
	}
	tc.recorder.Eventf(ref, v1.EventTypeNormal, "TaintManagerEviction", "Marking for deletion Pod %s", nsName.String())
}

// emitCancelPodDeletionEvent 记录取消删除 Pod 的事件；未配置 recorder 时静默跳过。
func (tc *Controller) emitCancelPodDeletionEvent(nsName types.NamespacedName) {
	if tc.recorder == nil {
		return
	}
	ref := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       nsName.Name,
		Namespace:  nsName.Namespace,
	}
	tc.recorder.Eventf(ref, v1.EventTypeNormal, "TaintManagerEviction", "Cancelling deletion of Pod %s", nsName.String())
}
