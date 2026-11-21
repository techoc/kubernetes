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

// TTLController 根据集群规模为 Node 设置 ttl 注解。
// Kubelet 会读取这些注解，将其作为重新向 apiserver 获取对象（如 Secret 或
// ConfigMap）之前可以缓存这些对象多久的建议。
//
// TODO: 这是针对 Kubelet 无法发送"监听本节点 Pod 所挂载的 Secret"请求的
// 临时规避方案。一旦能够发送此类请求，我们将修改 Kubelet 使用它，并彻底
// 移除本控制器。

package ttl

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"k8s.io/klog/v2"
)

// Controller 根据集群规模为 Node 设置 ttl 注解。
type Controller struct {
	kubeClient clientset.Interface

	// nodeStore 是 Node 的本地缓存。
	nodeStore listers.NodeLister

	// queue 中是需要同步的 Node。
	queue workqueue.TypedRateLimitingInterface[string]

	// hasSynced 在所有底层 informer 均已完成同步时返回 true。
	hasSynced func() bool

	lock sync.RWMutex

	// nodeCount 是集群中的 Node 数量。
	nodeCount int

	// desiredTTLSeconds 是集群中所有 Node 期望的 TTL（秒）。
	desiredTTLSeconds int

	// boundaryStep 表示当前所处的集群规模区间。
	boundaryStep int
}

// NewTTLController 创建一个新的 TTLController。
func NewTTLController(ctx context.Context, nodeInformer informers.NodeInformer, kubeClient clientset.Interface) *Controller {
	ttlc := &Controller{
		kubeClient: kubeClient,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "ttlcontroller"},
		),
	}
	logger := klog.FromContext(ctx)
	// 通过 informer 事件同时维护节点计数和待同步队列。
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ttlc.addNode(logger, obj)
		},
		UpdateFunc: func(old, newObj interface{}) {
			ttlc.updateNode(logger, old, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			ttlc.deleteNode(logger, obj)
		},
	})

	ttlc.nodeStore = listers.NewNodeLister(nodeInformer.Informer().GetIndexer())
	ttlc.hasSynced = nodeInformer.Informer().HasSynced

	return ttlc
}

// ttlBoundary 定义一个集群规模区间及对应的 TTL。
type ttlBoundary struct {
	sizeMin    int
	sizeMax    int
	ttlSeconds int
}

var (
	// 各档区间有意重叠，避免节点数在边界附近波动时频繁切换 TTL。
	ttlBoundaries = []ttlBoundary{
		{sizeMin: 0, sizeMax: 100, ttlSeconds: 0},
		{sizeMin: 90, sizeMax: 500, ttlSeconds: 15},
		{sizeMin: 450, sizeMax: 1000, ttlSeconds: 30},
		{sizeMin: 900, sizeMax: 2000, ttlSeconds: 60},
		{sizeMin: 1800, sizeMax: math.MaxInt32, ttlSeconds: 300},
	}
)

// Run 启动监听与同步循环，直到 ctx 结束。
func (ttlc *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting TTL controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down TTL controller")
		ttlc.queue.ShutDown()
		wg.Wait()
	}()

	// 必须先等 informer 同步完成，否则 nodeStore 中的节点数据不完整，算出的 TTL 会偏小。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, ttlc.hasSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, ttlc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// addNode 处理 Node 添加事件：更新节点计数和 TTL 档位，并将节点入队以同步注解。
func (ttlc *Controller) addNode(logger klog.Logger, obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected object type passed to addNode", "type", fmt.Sprintf("%T", obj))
		return
	}

	func() {
		ttlc.lock.Lock()
		defer ttlc.lock.Unlock()
		ttlc.nodeCount++
		// 只有真正越过区间上界才升档，重叠的边界让节点数在临界值附近抖动时不会反复切换。
		if ttlc.nodeCount > ttlBoundaries[ttlc.boundaryStep].sizeMax {
			ttlc.boundaryStep++
			ttlc.desiredTTLSeconds = ttlBoundaries[ttlc.boundaryStep].ttlSeconds
		}
	}()
	ttlc.enqueueNode(logger, node)
}

// updateNode 处理 Node 更新事件，将节点入队以便重新检查其 TTL 注解。
func (ttlc *Controller) updateNode(logger klog.Logger, _, newObj interface{}) {
	node, ok := newObj.(*v1.Node)
	if !ok {
		utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected object type passed to updateNode", "type", fmt.Sprintf("%T", newObj))
		return
	}
	// 处理所有 Node 更新可确保集群规模变化时 TTL 注解也随之更新。
	// 这里依赖 Kubelet 每 10 秒（一般为每 X 秒）上报一次 Node 状态，
	// 因此必要情况下其 TTL 注解会在该周期内得到更新。
	ttlc.enqueueNode(logger, node)
}

// deleteNode 处理 Node 删除事件：递减节点计数并按需回退 TTL 档位。
func (ttlc *Controller) deleteNode(logger klog.Logger, obj interface{}) {
	_, ok := obj.(*v1.Node)
	if !ok {
		// informer 可能无法确定删除时的最终状态而送来墓碑对象，需从中取出原始 Node 校验类型。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected object type passed to deleteNode", "type", fmt.Sprintf("%T", obj))
			return
		}
		_, ok = tombstone.Obj.(*v1.Node)
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected object type in tombstone", "type", fmt.Sprintf("%T", tombstone.Obj))
			return
		}
	}

	func() {
		ttlc.lock.Lock()
		defer ttlc.lock.Unlock()
		ttlc.nodeCount--
		// 与 addNode 对称：节点数低于当前区间下界时才降档。
		if ttlc.nodeCount < ttlBoundaries[ttlc.boundaryStep].sizeMin {
			ttlc.boundaryStep--
			ttlc.desiredTTLSeconds = ttlBoundaries[ttlc.boundaryStep].ttlSeconds
		}
	}()
	// 节点已不存在，无需再将其入队打补丁。
}

// enqueueNode 根据 Node 的对象键将其加入工作队列。
func (ttlc *Controller) enqueueNode(logger klog.Logger, node *v1.Node) {
	key, err := controller.KeyFunc(node)
	if err != nil {
		// 无法得到对象键就没有可靠的队列标识，只能丢弃该事件。
		logger.Error(nil, "Couldn't get key for object", "object", klog.KObj(node))
		return
	}
	ttlc.queue.Add(key)
}

// worker 持续处理队列中的条目，直到队列被关闭。
func (ttlc *Controller) worker(ctx context.Context) {
	for ttlc.processItem(ctx) {
	}
}

// processItem 处理队列中的一个条目，失败时按限速策略重新入队。
func (ttlc *Controller) processItem(ctx context.Context) bool {
	key, quit := ttlc.queue.Get()
	if quit {
		return false
	}
	// 无论成功与否都要标记处理完成，否则该条目会一直停留在队列中。
	defer ttlc.queue.Done(key)

	err := ttlc.updateNodeIfNeeded(ctx, key)
	if err == nil {
		// 成功后清空失败计数，避免后续偶发失败被退避策略过度延迟。
		ttlc.queue.Forget(key)
		return true
	}

	// 失败后限速重试，避免持续失败时忙循环。
	ttlc.queue.AddRateLimited(key)
	utilruntime.HandleErrorWithContext(ctx, err, "Failed to process node TTL", "key", key)
	return true
}

// getDesiredTTLSeconds 返回当前期望的 TTL 秒数。
func (ttlc *Controller) getDesiredTTLSeconds() int {
	ttlc.lock.RLock()
	defer ttlc.lock.RUnlock()
	return ttlc.desiredTTLSeconds
}

// getIntFromAnnotation 从 Node 的指定注解中读取整数；注解缺失或值非法时返回 false。
func getIntFromAnnotation(ctx context.Context, node *v1.Node, annotationKey string) (int, bool) {
	if node.Annotations == nil {
		return 0, false
	}
	annotationValue, ok := node.Annotations[annotationKey]
	if !ok {
		return 0, false
	}
	intValue, err := strconv.Atoi(annotationValue)
	if err != nil {
		logger := klog.FromContext(ctx)
		logger.Info("Could not convert the value with annotation key for the node", "annotationValue",
			annotationValue, "annotationKey", annotationKey, "node", klog.KObj(node))
		return 0, false
	}
	return intValue, true
}

// setIntAnnotation 将整数值以字符串形式写入 Node 的指定注解。
func setIntAnnotation(node *v1.Node, annotationKey string, value int) {
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[annotationKey] = strconv.Itoa(value)
}

// patchNodeWithAnnotation 通过 StrategicMergePatch 更新 Node 注解，
// 只提交差异字段，避免整对象更新覆盖其他控制器并发写入的内容。
func (ttlc *Controller) patchNodeWithAnnotation(ctx context.Context, node *v1.Node, annotationKey string, value int) error {
	oldData, err := json.Marshal(node)
	if err != nil {
		return err
	}
	setIntAnnotation(node, annotationKey, value)
	newData, err := json.Marshal(node)
	if err != nil {
		return err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &v1.Node{})
	if err != nil {
		return err
	}
	_, err = ttlc.kubeClient.CoreV1().Nodes().Patch(ctx, node.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(2).Info("Failed to change ttl annotation for node", "node", klog.KObj(node), "err", err)
		return err
	}
	logger.V(2).Info("Changed ttl annotation", "node", klog.KObj(node), "TTL", time.Duration(value)*time.Second)
	return nil
}

// updateNodeIfNeeded 检查 Node 当前的 TTL 注解，仅在缺失或与期望值不符时才打补丁。
func (ttlc *Controller) updateNodeIfNeeded(ctx context.Context, key string) error {
	node, err := ttlc.nodeStore.Get(key)
	if err != nil {
		// Node 已从缓存中消失（例如已删除），无需再处理。
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	desiredTTL := ttlc.getDesiredTTLSeconds()
	currentTTL, ok := getIntFromAnnotation(ctx, node, v1.ObjectTTLAnnotationKey)
	// 值一致时跳过写操作，减少无意义的 API 请求。
	if ok && currentTTL == desiredTTL {
		return nil
	}

	// 对缓存对象深拷贝后再修改，避免污染 informer 的共享缓存。
	return ttlc.patchNodeWithAnnotation(ctx, node.DeepCopy(), v1.ObjectTTLAnnotationKey, desiredTTL)
}
