/*
Copyright 2016 The Kubernetes Authors.

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

package ipam

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/cidrset"
	controllerutil "k8s.io/kubernetes/pkg/controller/util/node"
)

type rangeAllocator struct {
	client clientset.Interface
	// clusterCIDRs 是控制器创建时传入的集群 CIDR 列表。
	clusterCIDRs []*net.IPNet
	// cidrSets 与 clusterCIDRs 一一对应，记录每个 CIDR 中哪些已分配、哪些空闲。
	cidrSets []*cidrset.CidrSet
	// nodeLister 由传入控制器的共享 informer 填充，用于 list/get Node。
	nodeLister corelisters.NodeLister
	// nodesSynced 在 Node 共享 informer 至少同步过一次后返回 true。
	nodesSynced cache.InformerSynced
	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder

	// queue 存放待处理的工作项，既能去重，也便于出错时按限流策略重新入队。
	queue workqueue.RateLimitingInterface
}

var _ CIDRAllocator = &rangeAllocator{}

// NewCIDRRangeAllocator 返回一个 CIDRAllocator，用于为每个 Node 分配 CIDR（从每个 clusterCIDR 中各取一个）。
// 调用方必须保证 subNetMaskSize 不小于集群 CIDR 的掩码长度。
// 调用方必须传入已有 Node 的列表，以便新分配器初始化其 CIDR 映射。
// 调用方必须保证 ClusterCIDRs 在语义上正确（例如非双栈传 1 个、双栈传 2 个等）。
// NodeList 仅在测试时为 nil。
func NewCIDRRangeAllocator(ctx context.Context, client clientset.Interface, nodeInformer informers.NodeInformer, allocatorParams CIDRAllocatorParams, nodeList *v1.NodeList) (CIDRAllocator, error) {
	logger := klog.FromContext(ctx)
	if client == nil {
		logger.Error(nil, "kubeClient is nil when starting CIDRRangeAllocator")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cidrAllocator"})

	// 为每个需要管理的 CIDR 创建一个 cidrSet，并通过下标与 clusterCIDR 一一对应。
	cidrSets := make([]*cidrset.CidrSet, len(allocatorParams.ClusterCIDRs))
	for idx, cidr := range allocatorParams.ClusterCIDRs {
		cidrSet, err := cidrset.NewCIDRSet(cidr, allocatorParams.NodeCIDRMaskSizes[idx])
		if err != nil {
			return nil, err
		}
		cidrSets[idx] = cidrSet
	}

	ra := &rangeAllocator{
		client:       client,
		clusterCIDRs: allocatorParams.ClusterCIDRs,
		cidrSets:     cidrSets,
		nodeLister:   nodeInformer.Lister(),
		nodesSynced:  nodeInformer.Informer().HasSynced,
		broadcaster:  eventBroadcaster,
		recorder:     recorder,
		queue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "cidrallocator_node"),
	}

	if allocatorParams.ServiceCIDR != nil {
		ra.filterOutServiceRange(logger, allocatorParams.ServiceCIDR)
	} else {
		logger.Info("No Service CIDR provided. Skipping filtering out service addresses")
	}

	if allocatorParams.SecondaryServiceCIDR != nil {
		ra.filterOutServiceRange(logger, allocatorParams.SecondaryServiceCIDR)
	} else {
		logger.Info("No Secondary Service CIDR provided. Skipping filtering out secondary service addresses")
	}

	if nodeList != nil {
		for _, node := range nodeList.Items {
			if len(node.Spec.PodCIDRs) == 0 {
				logger.V(4).Info("Node has no CIDR, ignoring", "node", klog.KObj(&node))
				continue
			}
			logger.V(4).Info("Node has CIDR, occupying it in CIDR map", "node", klog.KObj(&node), "podCIDR", node.Spec.PodCIDR)
			if err := ra.occupyCIDRs(&node); err != nil {
				// 出现此错误只有两种可能：
				// 1. podCIDRs 字段中是脏数据，重试无济于事；
				// 2. CIDR 超出范围，说明节点 CIDR 发生了变化。
				// 直接返回错误会让 controller-manager 持续重启，但这里无法自行恢复。
				return nil, err
			}
		}
	}

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				ra.queue.Add(key)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				ra.queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// 触发删除事件时对象已从 informer 缓存移除；而 Node 没有 finalizer，
			// 因此也不会收到 DeletionTimestamp 非零的更新事件，只能在这里释放 CIDR。
			node, ok := obj.(*v1.Node)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					utilruntime.HandleErrorWithContext(ctx, nil, "Could not get object from tombstone", "obj", obj)
					return
				}
				node, ok = tombstone.Obj.(*v1.Node)
				if !ok {
					utilruntime.HandleErrorWithContext(ctx, nil, "Tombstone contained object that is not a node", "type", fmt.Sprintf("%T", obj))
					return
				}
			}
			if err := ra.ReleaseCIDR(logger, node); err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "Error while processing CIDR Release")
			}
		},
	})

	return ra, nil
}

// Run 启动 CIDR 分配器，处理事件并运行 worker，直到 ctx 被取消。
func (r *rangeAllocator) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	// 启动事件处理管线。
	r.broadcaster.StartStructuredLogging(3)
	logger := klog.FromContext(ctx)
	logger.Info("Sending events to api server")
	r.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: r.client.CoreV1().Events("")})
	defer r.broadcaster.Shutdown()

	logger.Info("Starting range CIDR allocator")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down range CIDR allocator")
		r.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, r.nodesSynced) {
		return
	}

	for i := 0; i < cidrUpdateWorkers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, r.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 是长期运行的 worker 主循环，不断调用 processNextWorkItem 从队列中读取并处理工作项。
func (r *rangeAllocator) runWorker(ctx context.Context) {
	for r.processNextNodeWorkItem(ctx) {
	}
}

// processNextNodeWorkItem 从队列中取出一个工作项，并调用 syncNode 尝试处理。
func (r *rangeAllocator) processNextNodeWorkItem(ctx context.Context) bool {
	obj, shutdown := r.queue.Get()
	if shutdown {
		return false
	}

	// 把处理过程包在匿名函数中，这样就可以用 defer 保证 queue.Done 一定被调用。
	err := func(logger klog.Logger, obj interface{}) error {
		// Done 通知队列本工作项已处理完毕。若不想让该项重新入队，还必须调用 Forget。
		// 遇到瞬时错误时不调用 Forget，而是把条目重新入队，等待退避后重试。
		defer r.queue.Done(obj)
		var key string
		var ok bool
		// 队列中的条目应当是 namespace/name 形式的字符串。
		// 由于队列处理有延迟，处理时的 informer 缓存对象可能比入队时更新，因此这里只取 key 再实时查询。
		if key, ok = obj.(string); !ok {
			// 条目类型非法，必须调用 Forget，否则会陷入反复处理无效条目的死循环。
			r.queue.Forget(obj)
			utilruntime.HandleErrorWithContext(ctx, nil, "Expected string but got unexpected type in work node queue", "type", fmt.Sprintf("%T", obj))
			return nil
		}
		// 调用 syncNode 处理 key 对应的 Node。
		if err := r.syncNode(ctx, key); err != nil {
			// 出现错误时按限流策略重新入队，以应对瞬时故障。
			r.queue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// 处理成功则 Forget，直到下次对象变化前不再重复入队。
		r.queue.Forget(obj)
		logger.V(4).Info("Successfully synced", "key", key)
		return nil
	}(klog.FromContext(ctx), obj)

	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Error processing node work item")
		return true
	}

	return true
}

// syncNode 同步单个 Node：为未分配 CIDR 的节点分配或占用 CIDR。
func (r *rangeAllocator) syncNode(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing Node request", "node", key, "elapsed", time.Since(startTime))
	}()

	node, err := r.nodeLister.Get(key)
	if apierrors.IsNotFound(err) {
		logger.V(3).Info("node has been deleted", "node", key)
		// TODO: 获取节点对象信息，从这里调用 ReleaseCIDR，并在出错时重试。
		return nil
	}
	if err != nil {
		return err
	}
	// 通过 DeletionTimestamp 判断对象是否正处于删除流程中，正在删除的节点无需分配。
	if !node.DeletionTimestamp.IsZero() {
		logger.V(3).Info("node is being deleted", "node", key)
		return nil
	}
	return r.AllocateOrOccupyCIDR(ctx, node)
}

// occupyCIDRs 将 node.PodCIDRs 中已有的 CIDR 在分配器维护的 cidrSet 中标记为已使用。
func (r *rangeAllocator) occupyCIDRs(node *v1.Node) error {
	if len(node.Spec.PodCIDRs) == 0 {
		return nil
	}
	for idx, cidr := range node.Spec.PodCIDRs {
		_, podCIDR, err := netutils.ParseCIDRSloppy(cidr)
		if err != nil {
			return fmt.Errorf("failed to parse node %s, CIDR %s", node.Name, node.Spec.PodCIDR)
		}
		// 节点上预分配的 CIDR 下标超出了当前配置的 CIDR 范围。
		// 这通常发生在集群从双栈（多个 CIDR）改为非双栈时，此时已无从追踪和释放该 CIDR。
		if idx >= len(r.cidrSets) {
			return fmt.Errorf("node:%s has an allocated cidr: %v at index:%v that does not exist in cluster cidrs configuration", node.Name, cidr, idx)
		}

		if err := r.cidrSets[idx].Occupy(podCIDR); err != nil {
			return fmt.Errorf("failed to mark cidr[%v] at idx [%v] as occupied for node: %v: %v", podCIDR, idx, node.Name, err)
		}
	}
	return nil
}

// AllocateOrOccupyCIDR 为节点分配新 CIDR，或将节点上已有的 CIDR 标记为已占用。
// 注意：如果在本函数中新增返回路径或 defer 操作，必须确保在处理完成时正确更新 nodesInProcessing。
func (r *rangeAllocator) AllocateOrOccupyCIDR(ctx context.Context, node *v1.Node) error {
	if node == nil {
		return nil
	}

	if len(node.Spec.PodCIDRs) > 0 {
		return r.occupyCIDRs(node)
	}

	logger := klog.FromContext(ctx)
	allocatedCIDRs := make([]*net.IPNet, len(r.cidrSets))

	for idx := range r.cidrSets {
		podCIDR, err := r.cidrSets[idx].AllocateNext()
		if err != nil {
			controllerutil.RecordNodeStatusChange(logger, r.recorder, node, "CIDRNotAvailable")
			return fmt.Errorf("failed to allocate cidr from cluster cidr at idx:%v: %v", idx, err)
		}
		allocatedCIDRs[idx] = podCIDR
	}

	// 把 CIDR 分配结果提交给 API server（通过工作队列触发更新）。
	logger.V(4).Info("Putting node with CIDR into the work queue", "node", klog.KObj(node), "CIDRs", allocatedCIDRs)
	return r.updateCIDRsAllocation(ctx, node.Name, allocatedCIDRs)
}

// ReleaseCIDR 将 node.PodCIDRs 中的 CIDR 在 cidrSet 中标记为未使用，供后续分配复用。
func (r *rangeAllocator) ReleaseCIDR(logger klog.Logger, node *v1.Node) error {
	if node == nil || len(node.Spec.PodCIDRs) == 0 {
		return nil
	}

	for idx, cidr := range node.Spec.PodCIDRs {
		_, podCIDR, err := netutils.ParseCIDRSloppy(cidr)
		if err != nil {
			return fmt.Errorf("failed to parse CIDR %s on Node %v: %v", cidr, node.Name, err)
		}

		// 节点上预分配的 CIDR 下标超出了当前配置的 CIDR 范围。
		// 这通常发生在集群从双栈（多个 CIDR）改为非双栈时，此时已无从追踪和释放该 CIDR。
		if idx >= len(r.cidrSets) {
			return fmt.Errorf("node:%s has an allocated cidr: %v at index:%v that does not exist in cluster cidrs configuration", node.Name, cidr, idx)
		}

		logger.V(4).Info("Release CIDR for node", "CIDR", cidr, "node", klog.KObj(node))
		if err = r.cidrSets[idx].Release(podCIDR); err != nil {
			return fmt.Errorf("error when releasing CIDR %v: %v", cidr, err)
		}
	}
	return nil
}

// filterOutServiceRange 把属于 serviceCIDR 的 CIDR 在所有 cidrSet 中标记为已使用，防止其被分配给 Pod。
func (r *rangeAllocator) filterOutServiceRange(logger klog.Logger, serviceCIDR *net.IPNet) {
	// 判断 Service CIDR 与集群 CIDR 是否有交集：只要一方应用自身掩码后包含另一方，
	// 就说明两者存在包含关系，需要过滤。
	for idx, cidr := range r.clusterCIDRs {
		// 两者没有交集时无需过滤。
		if !cidr.Contains(serviceCIDR.IP.Mask(cidr.Mask)) && !serviceCIDR.Contains(cidr.IP.Mask(serviceCIDR.Mask)) {
			continue
		}

		// 此处 cidrSet 与 clusterCIDR 的长度必然相等，可以按同一索引访问。
		if err := r.cidrSets[idx].Occupy(serviceCIDR); err != nil {
			logger.Error(err, "Error filtering out service cidr out cluster cidr", "CIDR", cidr, "index", idx, "serviceCIDR", serviceCIDR)
		}
	}
}

// updateCIDRsAllocation 将分配好的 CIDR 写入 Node 并提交到 API server。
func (r *rangeAllocator) updateCIDRsAllocation(ctx context.Context, nodeName string, allocatedCIDRs []*net.IPNet) error {
	var err error
	var node *v1.Node
	logger := klog.FromContext(ctx)
	cidrsString := ipnetToStringList(allocatedCIDRs)
	node, err = r.nodeLister.Get(nodeName)
	if err != nil {
		logger.Error(err, "Failed while getting node for updating Node.Spec.PodCIDRs", "node", klog.KRef("", nodeName))
		return err
	}

	// 若节点上已有的 CIDR 与本次要写入的完全一致，说明之前可能已经更新成功，
	// 只是没来得及确认，此时直接返回即可，避免重复写入。
	if len(node.Spec.PodCIDRs) == len(allocatedCIDRs) {
		match := true
		for idx, cidr := range cidrsString {
			if node.Spec.PodCIDRs[idx] != cidr {
				match = false
				break
			}
		}
		if match {
			logger.V(4).Info("Node already has allocated CIDR. It matches the proposed one", "node", klog.KObj(node), "CIDRs", allocatedCIDRs)
			return nil
		}
	}

	// 节点已有 CIDR，说明本次分配是多余的，需将刚预留的 CIDR 释放回池。
	if len(node.Spec.PodCIDRs) != 0 {
		logger.Error(nil, "Node already has a CIDR allocated. Releasing the new one", "node", klog.KObj(node), "podCIDRs", node.Spec.PodCIDRs)
		for idx, cidr := range allocatedCIDRs {
			if releaseErr := r.cidrSets[idx].Release(cidr); releaseErr != nil {
				logger.Error(releaseErr, "Error when releasing CIDR", "index", idx, "CIDR", cidr)
			}
		}
		return nil
	}

	// 能执行到这里说明节点尚未分配 CIDR，尝试写入。
	for i := 0; i < cidrUpdateRetries; i++ {
		if err = nodeutil.PatchNodeCIDRs(ctx, r.client, types.NodeName(node.Name), cidrsString); err == nil {
			logger.Info("Set node PodCIDR", "node", klog.KObj(node), "podCIDRs", cidrsString)
			return nil
		}
	}
	// 多次重试后仍失败，准备将 CIDR 释放回池。
	logger.Error(err, "Failed to update node PodCIDR after multiple attempts", "node", klog.KObj(node), "podCIDRs", cidrsString)
	controllerutil.RecordNodeStatusChange(logger, r.recorder, node, "CIDRAssignmentFailed")
	// 这里接受可能泄漏 CIDR 的事实：在无法确定请求是否送达服务端时，
	// 贸然释放比泄漏更危险。NodeController 重启时会把所有误分配的 CIDR 归还到池中。
	// 只有明确不是服务端超时（即请求大概率未生效）时才释放。
	if !apierrors.IsServerTimeout(err) {
		logger.Error(err, "CIDR assignment for node failed. Releasing allocated CIDR", "node", klog.KObj(node))
		for idx, cidr := range allocatedCIDRs {
			if releaseErr := r.cidrSets[idx].Release(cidr); releaseErr != nil {
				logger.Error(releaseErr, "Error releasing allocated CIDR for node", "node", klog.KObj(node))
			}
		}
	}
	return err
}
