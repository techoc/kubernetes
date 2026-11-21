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

package podgc

import (
	"context"
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/podgc/metrics"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	nodeutil "k8s.io/kubernetes/pkg/util/node"
	utilpod "k8s.io/kubernetes/pkg/util/pod"
	"k8s.io/kubernetes/pkg/util/taints"
)

const (
	// gcCheckPeriod 定义主控制器的运行周期。
	gcCheckPeriod = 20 * time.Second
	// quarantineTime 定义 Orphaned GC 的隔离时长：节点从 informer 中消失后，
	// 需等待这么久才发起 GET 请求确认它是否真的被删除，避免 informer 短暂延迟
	// 导致误删 Pod。
	quarantineTime = 40 * time.Second
)

// PodGCController 负责垃圾回收处于终止状态、被驱逐、绑定到已消失节点或未调度的 Pod。
type PodGCController struct {
	// kubeClient 用于获取节点并强制删除 Pod。
	kubeClient clientset.Interface

	// podLister/podListerSynced 提供 Pod 的本地缓存视图及同步状态。
	podLister       corelisters.PodLister
	podListerSynced cache.InformerSynced
	// nodeLister/nodeListerSynced 提供 Node 的本地缓存视图及同步状态。
	nodeLister       corelisters.NodeLister
	nodeListerSynced cache.InformerSynced

	// nodeQueue 延迟队列：节点从 informer 中消失后先隔离一段时间再核查。
	nodeQueue workqueue.TypedDelayingInterface[string]

	// terminatedPodThreshold 是允许保留的已终止 Pod 数量上限，超过才开始回收。
	terminatedPodThreshold int
	// gcCheckPeriod 是主循环的触发周期。
	gcCheckPeriod time.Duration
	// quarantineTime 是孤立节点在核查前的隔离时长。
	quarantineTime time.Duration
}

// NewPodGC 使用默认的检查周期和隔离时长构造 PodGCController。
func NewPodGC(ctx context.Context, kubeClient clientset.Interface, podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer, terminatedPodThreshold int) *PodGCController {
	return NewPodGCInternal(ctx, kubeClient, podInformer, nodeInformer, terminatedPodThreshold, gcCheckPeriod, quarantineTime)
}

// NewPodGCInternal 允许自定义检查周期和隔离时长，仅用于集成测试。
func NewPodGCInternal(ctx context.Context, kubeClient clientset.Interface, podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer, terminatedPodThreshold int, gcCheckPeriod, quarantineTime time.Duration) *PodGCController {
	gcc := &PodGCController{
		kubeClient:             kubeClient,
		terminatedPodThreshold: terminatedPodThreshold,
		podLister:              podInformer.Lister(),
		podListerSynced:        podInformer.Informer().HasSynced,
		nodeLister:             nodeInformer.Lister(),
		nodeListerSynced:       nodeInformer.Informer().HasSynced,
		nodeQueue:              workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Name: "orphaned_pods_nodes"}),
		gcCheckPeriod:          gcCheckPeriod,
		quarantineTime:         quarantineTime,
	}

	// 注册 Prometheus 指标，供后续垃圾回收过程中计数。
	metrics.RegisterMetrics()
	return gcc
}

// Run 等待 informer 缓存同步后，按 gcCheckPeriod 周期执行垃圾回收，直到 ctx 结束。
func (gcc *PodGCController) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("Starting GC controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down GC controller")
		gcc.nodeQueue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, gcc.podListerSynced, gcc.nodeListerSynced) {
		return
	}

	wg.Go(func() {
		wait.UntilWithContext(ctx, gcc.gc, gcc.gcCheckPeriod)
	})
	<-ctx.Done()
}

// gc 执行一次完整的垃圾回收：依次处理已终止、终止中、孤立和未调度的 Pod。
func (gcc *PodGCController) gc(ctx context.Context) {
	pods, err := gcc.podLister.List(labels.Everything())
	if err != nil {
		klog.FromContext(ctx).Error(err, "Error while listing all pods")
		return
	}
	nodes, err := gcc.nodeLister.List(labels.Everything())
	if err != nil {
		klog.FromContext(ctx).Error(err, "Error while listing all nodes")
		return
	}
	if gcc.terminatedPodThreshold > 0 {
		gcc.gcTerminated(ctx, pods)
	}
	gcc.gcTerminating(ctx, pods)
	gcc.gcOrphaned(ctx, pods, nodes)
	gcc.gcUnscheduledTerminating(ctx, pods)
}

// isPodTerminated 判断 Pod 是否处于终态（即不属于 Pending/Running/Unknown 阶段）。
func isPodTerminated(pod *v1.Pod) bool {
	if phase := pod.Status.Phase; phase != v1.PodPending && phase != v1.PodRunning && phase != v1.PodUnknown {
		return true
	}
	return false
}

// isPodTerminating 通过删除时间戳判断 Pod 是否正在终止。
func isPodTerminating(pod *v1.Pod) bool {
	return pod.ObjectMeta.DeletionTimestamp != nil
}

// gcTerminating 回收位于 NotReady 且带有 out-of-service 污点节点上的终止中 Pod，
// 以便节点恢复前释放这些 Pod 占用的资源。
func (gcc *PodGCController) gcTerminating(ctx context.Context, pods []*v1.Pod) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("GC'ing terminating pods that are on out-of-service nodes")
	terminatingPods := []*v1.Pod{}
	for _, pod := range pods {
		if isPodTerminating(pod) {
			node, err := gcc.nodeLister.Get(pod.Spec.NodeName)
			if err != nil {
				logger.Error(err, "Failed to get node", "node", klog.KRef("", pod.Spec.NodeName))
				continue
			}
			// 只有同时满足以下两个条件的 Pod 才回收：
			// 1. 节点处于 NotReady 状态；
			// 2. 节点带有 node.kubernetes.io/out-of-service 污点。
			// 两者同时成立才说明节点被有意摘除，避免误删正常节点上的 Pod。
			if !nodeutil.IsNodeReady(node) && taints.TaintKeyExists(node.Spec.Taints, v1.TaintNodeOutOfService) {
				logger.V(4).Info("Garbage collecting pod that is terminating", "pod", klog.KObj(pod), "phase", pod.Status.Phase)
				terminatingPods = append(terminatingPods, pod)
			}
		}
	}

	deleteCount := len(terminatingPods)
	if deleteCount == 0 {
		return
	}

	logger.V(4).Info("Garbage collecting pods that are terminating on node tainted with node.kubernetes.io/out-of-service", "numPods", deleteCount)
	// 先删除被驱逐的 Pod、再按创建时间排序，避免影响控制器新建的 Pod。
	sort.Sort(byEvictionAndCreationTimestamp(terminatingPods))
	var wait sync.WaitGroup
	for i := 0; i < deleteCount; i++ {
		wait.Add(1)
		go func(pod *v1.Pod) {
			defer wait.Done()
			metrics.DeletingPodsTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonTerminatingOutOfService).Inc()
			if err := gcc.markFailedAndDeletePod(ctx, pod); err != nil {
				// Pod 可能已被其他组件删除，记录错误即可，无需重试。
				utilruntime.HandleErrorWithContext(ctx, err, "Failed to delete terminating pod on out-of-service node", "pod", klog.KObj(pod))
				metrics.DeletingPodsErrorTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonTerminatingOutOfService).Inc()
			}
		}(terminatingPods[i])
	}
	wait.Wait()
}

// gcTerminated 当终态 Pod 数量超过阈值时，删除最老的一批（优先删除被驱逐的），
// 以释放集群资源。
func (gcc *PodGCController) gcTerminated(ctx context.Context, pods []*v1.Pod) {
	terminatedPods := []*v1.Pod{}
	for _, pod := range pods {
		if isPodTerminated(pod) {
			terminatedPods = append(terminatedPods, pod)
		}
	}

	terminatedPodCount := len(terminatedPods)
	deleteCount := terminatedPodCount - gcc.terminatedPodThreshold

	if deleteCount <= 0 {
		return
	}

	logger := klog.FromContext(ctx)
	logger.Info("Garbage collecting pods", "numPods", deleteCount)
	// 先删除被驱逐的 Pod，避免它们继续占用已终止 Pod 的配额。
	sort.Sort(byEvictionAndCreationTimestamp(terminatedPods))
	var wait sync.WaitGroup
	for i := 0; i < deleteCount; i++ {
		wait.Add(1)
		go func(pod *v1.Pod) {
			defer wait.Done()
			if err := gcc.markFailedAndDeletePod(ctx, pod); err != nil {
				// Pod 可能已被其他组件删除，记录错误即可，无需重试。
				defer utilruntime.HandleErrorWithContext(ctx, err, "Failed to delete terminated pod", "pod", klog.KObj(pod))
				metrics.DeletingPodsErrorTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonTerminated).Inc()
			}
			metrics.DeletingPodsTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonTerminated).Inc()
		}(terminatedPods[i])
	}
	wait.Wait()
}

// gcOrphaned 删除绑定到已不存在节点上的 Pod，删除前会为这些 Pod 设置
// DisruptionTarget 条件以说明原因，删除前先经过隔离期确认节点确实消失。
func (gcc *PodGCController) gcOrphaned(ctx context.Context, pods []*v1.Pod, nodes []*v1.Node) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("GC'ing orphaned")
	existingNodeNames := sets.NewString()
	for _, node := range nodes {
		existingNodeNames.Insert(node.Name)
	}
	// 将 informer 中找不到但被 Pod 引用的节点加入延迟队列，隔离一段时间后再核查，
	// 避免 informer 短暂延迟导致误判节点已删除。
	for _, pod := range pods {
		if pod.Spec.NodeName != "" && !existingNodeNames.Has(pod.Spec.NodeName) {
			gcc.nodeQueue.AddAfter(pod.Spec.NodeName, gcc.quarantineTime)
		}
	}
	// 检查隔离期结束后节点是否仍然缺失。
	deletedNodesNames, quit := gcc.discoverDeletedNodes(ctx, existingNodeNames)
	if quit {
		return
	}
	// 删除被判定为孤儿的 Pod。
	for _, pod := range pods {
		if !deletedNodesNames.Has(pod.Spec.NodeName) {
			continue
		}
		logger.V(2).Info("Found orphaned Pod assigned to the Node, deleting", "pod", klog.KObj(pod), "node", klog.KRef("", pod.Spec.NodeName))
		condition := &v1.PodCondition{
			Type:               v1.DisruptionTarget,
			ObservedGeneration: apipod.CalculatePodConditionObservedGeneration(&pod.Status, pod.Generation, v1.DisruptionTarget),
			Status:             v1.ConditionTrue,
			Reason:             "DeletionByPodGC",
			Message:            "PodGC: node no longer exists",
		}
		if err := gcc.markFailedAndDeletePodWithCondition(ctx, pod, condition); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Failed to delete orphaned pod", "pod", klog.KObj(pod))
			metrics.DeletingPodsErrorTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonOrphaned).Inc()
		} else {
			logger.Info("Forced deletion of orphaned Pod succeeded", "pod", klog.KObj(pod))
		}
		metrics.DeletingPodsTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonOrphaned).Inc()
	}
}

// discoverDeletedNodes 消费隔离队列，通过 API 二次确认哪些节点确实已被删除；
// 第二个返回值表示队列已关闭、控制器正在退出。
func (gcc *PodGCController) discoverDeletedNodes(ctx context.Context, existingNodeNames sets.String) (sets.String, bool) {
	deletedNodesNames := sets.NewString()
	for gcc.nodeQueue.Len() > 0 {
		item, quit := gcc.nodeQueue.Get()
		if quit {
			return nil, true
		}
		nodeName := item
		if !existingNodeNames.Has(nodeName) {
			exists, err := gcc.checkIfNodeExists(ctx, nodeName)
			switch {
			case err != nil:
				klog.FromContext(ctx).Error(err, "Error while getting node", "node", klog.KRef("", nodeName))
				// 查询失败时无需重新入队：若节点确实缺失，后续 gc 循环会再次加入。
			case !exists:
				deletedNodesNames.Insert(nodeName)
			}
		}
		gcc.nodeQueue.Done(item)
	}
	return deletedNodesNames, false
}

// checkIfNodeExists 直接向 API Server 查询节点是否存在，NotFound 视为不存在且不返回错误。
func (gcc *PodGCController) checkIfNodeExists(ctx context.Context, name string) (bool, error) {
	_, fetchErr := gcc.kubeClient.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if errors.IsNotFound(fetchErr) {
		return false, nil
	}
	return fetchErr == nil, fetchErr
}

// gcUnscheduledTerminating 删除正在终止但从未绑定到节点的 Pod；这类 Pod 不会
// 被 kubelet 清理，只能由控制器强制删除。
func (gcc *PodGCController) gcUnscheduledTerminating(ctx context.Context, pods []*v1.Pod) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("GC'ing unscheduled pods which are terminating")

	for _, pod := range pods {
		if pod.DeletionTimestamp == nil || len(pod.Spec.NodeName) > 0 {
			continue
		}

		logger.V(2).Info("Found unscheduled terminating Pod not assigned to any Node, deleting", "pod", klog.KObj(pod))
		if err := gcc.markFailedAndDeletePod(ctx, pod); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Failed to delete unscheduled terminating pod", "pod", klog.KObj(pod))
			metrics.DeletingPodsErrorTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonTerminatingUnscheduled).Inc()
		} else {
			logger.Info("Forced deletion of unscheduled terminating Pod succeeded", "pod", klog.KObj(pod))
		}
		metrics.DeletingPodsTotal.WithLabelValues(pod.Namespace, metrics.PodGCReasonTerminatingUnscheduled).Inc()
	}
}

// byEvictionAndCreationTimestamp 先按是否被驱逐、再按创建时间对 Pod 排序，
// 名称作为平局判定依据。被驱逐的 Pod 排在前面先删除，避免影响控制器新建的 Pod。
type byEvictionAndCreationTimestamp []*v1.Pod

// Len 返回待排序 Pod 的数量。
func (o byEvictionAndCreationTimestamp) Len() int { return len(o) }

// Swap 交换第 i 个和第 j 个 Pod。
func (o byEvictionAndCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 判断第 i 个 Pod 是否应排在第 j 个之前：被驱逐的优先，其次创建时间更早的。
func (o byEvictionAndCreationTimestamp) Less(i, j int) bool {
	iEvicted, jEvicted := eviction.PodIsEvicted(o[i].Status), eviction.PodIsEvicted(o[j].Status)
	// 被驱逐的 Pod 视为“更小”，从而优先被删除。
	if iEvicted != jEvicted {
		return iEvicted
	}
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// markFailedAndDeletePod 将 Pod 标记为 Failed 后立即删除（使用 0 秒优雅期）。
func (gcc *PodGCController) markFailedAndDeletePod(ctx context.Context, pod *v1.Pod) error {
	return gcc.markFailedAndDeletePodWithCondition(ctx, pod, nil)
}

// markFailedAndDeletePodWithCondition 在删除前将 Pod 状态置为 Failed，并可附加一个
// 条件说明删除原因；对孤立 Pod 尤其重要，否则没有 kubelet 会修改其阶段，它会永远停留在 Running。
func (gcc *PodGCController) markFailedAndDeletePodWithCondition(ctx context.Context, pod *v1.Pod, condition *v1.PodCondition) error {
	logger := klog.FromContext(ctx)
	logger.Info("PodGC is force deleting Pod", "pod", klog.KObj(pod))
	// 删除前先 Patch 状态，确保 Pod 进入 Failed 阶段。
	//
	// 对孤立 Pod 而言这一步尤为关键：没有 kubelet 在运行来更新阶段，
	// 若不主动标记，Pod 会永远停留在 Running 阶段。
	if pod.Status.Phase != v1.PodSucceeded && pod.Status.Phase != v1.PodFailed {
		newStatus := pod.Status.DeepCopy()
		newStatus.Phase = v1.PodFailed
		newStatus.ObservedGeneration = apipod.CalculatePodStatusObservedGeneration(pod)
		if condition != nil {
			apipod.UpdatePodCondition(newStatus, condition)
		}
		if _, _, _, err := utilpod.PatchPodStatus(ctx, gcc.kubeClient, pod.Namespace, pod.Name, pod.UID, pod.Status, *newStatus); err != nil {
			return err
		}
	}
	return gcc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
}
