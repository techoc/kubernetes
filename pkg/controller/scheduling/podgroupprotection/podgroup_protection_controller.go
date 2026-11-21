/*
Copyright The Kubernetes Authors.

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

package podgroupprotection

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/controller/util/protectionutil"
	"k8s.io/kubernetes/pkg/util/slice"
)

const (
	// activePodSchedulingGroupIndex 是用于按 schedulingGroup.podGroupName 字段查找活跃 Pod 的索引名。
	activePodSchedulingGroupIndex = "activePodSchedulingGroup"
)

// Controller 负责管理 PodGroup 对象上的 PodGroupProtectionFinalizer。
// 该 finalizer 在创建时由 PodGroupProtection 准入插件打上；当 PodGroup 正在被删除、
// 且不再有活跃的（未终止的）Pod 引用它时，本控制器会将其移除。
type Controller struct {
	kubeClient clientset.Interface

	podGroupLister schedulinglisters.PodGroupLister
	podGroupSynced cache.InformerSynced

	podSynced cache.InformerSynced

	// podIndexer 安装了通用的 Pod 索引，用于把遍历范围缩小到我们关心的 Pod。
	podIndexer cache.Indexer

	queue workqueue.TypedRateLimitingInterface[string]
}

// NewPodGroupProtectionController 创建一个 PodGroup 保护控制器实例。
func NewPodGroupProtectionController(
	logger klog.Logger,
	podGroupInformer schedulinginformers.PodGroupInformer,
	podInformer coreinformers.PodInformer,
	kubeClient clientset.Interface,
) (*Controller, error) {
	c := &Controller{
		kubeClient:     kubeClient,
		podGroupLister: podGroupInformer.Lister(),
		podGroupSynced: podGroupInformer.Informer().HasSynced,
		podIndexer:     podInformer.Informer().GetIndexer(),
		podSynced:      podInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "podgroupprotection"},
		),
	}

	if _, err := podGroupInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.handlePodGroupUpdate(logger, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			c.handlePodGroupUpdate(logger, new)
		},
	}, cache.HandlerOptions{Logger: &logger}); err != nil {
		return nil, err
	}

	if err := addActivePodSchedulingGroupIndexer(c.podIndexer); err != nil {
		return nil, fmt.Errorf("could not initialize PodGroup protection controller: %w", err)
	}

	if _, err := podInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.handlePodChange(logger, nil, obj)
		},
		DeleteFunc: func(obj interface{}) {
			c.handlePodChange(logger, obj, nil)
		},
		UpdateFunc: func(old, new interface{}) {
			c.handlePodChange(logger, old, new)
		},
	}, cache.HandlerOptions{Logger: &logger}); err != nil {
		return nil, err
	}

	return c, nil
}

// addActivePodSchedulingGroupIndexer 为 Pod 索引器添加一个按 schedulingGroup.podGroupName
// 字段查找活跃 Pod 的索引，从而能高效判断某个 PodGroup 是否仍有活跃 Pod 在使用。
func addActivePodSchedulingGroupIndexer(indexer cache.Indexer) error {
	return indexer.AddIndexers(cache.Indexers{
		activePodSchedulingGroupIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				// 索引函数只对 Pod 生效，其他类型直接忽略。
				return nil, nil
			}
			if isPodTerminated(pod) {
				// 已终止的 Pod 不再持有 PodGroup，排除出索引。
				return nil, nil
			}
			if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
				return nil, nil
			}
			return []string{pod.Namespace + "/" + *pod.Spec.SchedulingGroup.PodGroupName}, nil
		},
	})
}

// Run 启动控制器的各个 worker goroutine，并阻塞直到 ctx 被取消。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting PodGroup protection controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down PodGroup protection controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.podGroupSynced, c.podSynced) {
		return
	}

	for range workers {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 是单个 worker 的主循环，持续处理队列中的条目。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出并处理一个 PodGroup；成功则清除限流状态，失败则限流重试。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	pgKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(pgKey)

	err := c.processPodGroup(ctx, pgKey)
	if err == nil {
		c.queue.Forget(pgKey)
		return true
	}

	c.queue.AddRateLimited(pgKey)
	utilruntime.HandleError(fmt.Errorf("PodGroup %v failed with: %w", pgKey, err))

	return true
}

// processPodGroup 处理单个 PodGroup：处于删除候选状态且无活跃 Pod 引用时移除保护 finalizer。
func (c *Controller) processPodGroup(ctx context.Context, pgKey string) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Processing PodGroup", "podGroup", pgKey)

	pgNamespace, pgName, err := cache.SplitMetaNamespaceKey(pgKey)
	if err != nil {
		return fmt.Errorf("error parsing PodGroup key %q: %w", pgKey, err)
	}

	pg, err := c.podGroupLister.PodGroups(pgNamespace).Get(pgName)
	if apierrors.IsNotFound(err) {
		logger.V(4).Info("PodGroup not found, ignoring", "podGroup", pgKey)
		return nil
	}
	if err != nil {
		return err
	}

	// 只有正在删除且仍带有保护 finalizer 的 PodGroup 才需要处理。
	if !protectionutil.IsDeletionCandidate(pg, scheduling.PodGroupProtectionFinalizer) {
		return nil
	}

	isUsed, err := c.hasActivePods(ctx, pg)
	if err != nil {
		return err
	}
	if !isUsed {
		return c.removeFinalizer(ctx, pg)
	}
	logger.V(4).Info("Keeping PodGroup finalizer because it is still being used by pods", "podGroup", klog.KObj(pg))
	return nil
}

// removeFinalizer 从 PodGroup 上移除保护 finalizer，使其真正被删除。
func (c *Controller) removeFinalizer(ctx context.Context, pg *schedulingv1beta1.PodGroup) error {
	logger := klog.FromContext(ctx)
	// 在深拷贝上修改，绝不改动 informer 缓存中的对象。
	pgClone := pg.DeepCopy()

	pgClone.Finalizers = slice.RemoveString(pgClone.Finalizers, scheduling.PodGroupProtectionFinalizer, nil)
	_, err := c.kubeClient.SchedulingV1beta1().PodGroups(pgClone.Namespace).Update(ctx, pgClone, metav1.UpdateOptions{})
	if err != nil {
		logger.Error(err, "Error removing protection finalizer from PodGroup", "podGroup", klog.KObj(pg))
		return err
	}

	logger.V(3).Info("Removed protection finalizer from PodGroup", "podGroup", klog.KObj(pg))
	return nil
}

// hasActivePods 判断是否有活跃 Pod 通过 spec.schedulingGroup.podGroupName 引用该 PodGroup。
// 索引中只包含未终止的 Pod，因此查询结果非空即可说明该 PodGroup 仍在使用中。
func (c *Controller) hasActivePods(ctx context.Context, pg *schedulingv1beta1.PodGroup) (bool, error) {
	logger := klog.FromContext(ctx)
	indexKey := pg.Namespace + "/" + pg.Name

	objs, err := c.podIndexer.ByIndex(activePodSchedulingGroupIndex, indexKey)
	if err != nil {
		return false, fmt.Errorf("index-based list of active pods failed for PodGroup %s: %w", indexKey, err)
	}

	if len(objs) > 0 {
		logger.V(4).Info("Pod is using PodGroup", "pod", klog.KObj(objs[0].(*v1.Pod)), "podGroup", klog.KObj(pg))
		return true, nil
	}

	logger.V(4).Info("No active pods found using PodGroup", "podGroup", klog.KObj(pg))
	return false, nil
}

// isPodTerminated 判断 Pod 是否已经结束（Succeeded 或 Failed）。
func isPodTerminated(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed
}

// handlePodGroupUpdate 处理 PodGroup 的新增/更新事件。
// 只有正在删除且带有 finalizer 的删除候选对象才需要处理。
func (c *Controller) handlePodGroupUpdate(logger klog.Logger, obj interface{}) {
	pg, ok := obj.(*schedulingv1beta1.PodGroup)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PodGroup informer returned non-PodGroup object: %#v", obj))
		return
	}
	if !protectionutil.IsDeletionCandidate(pg, scheduling.PodGroupProtectionFinalizer) {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pg)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for PodGroup %#v: %w", pg, err))
		return
	}
	logger.V(4).Info("Got event on PodGroup", "podGroup", klog.KObj(pg))
	c.queue.Add(key)
}

// handlePodChange 处理 Pod 的新增/删除/更新事件。
// 只有在事件可能影响 finalizer 决策时（Pod 被删除或进入终止阶段）才把引用的 PodGroup 入队。
func (c *Controller) handlePodChange(logger klog.Logger, old, new interface{}) {
	newPod := getPod(new)
	oldPod := getPod(old)

	if newPod != nil && isPodTerminated(newPod) {
		c.enqueuePodGroupForPod(logger, newPod)
	}

	// 一次更新通知可能掩盖了这样的过程：Pod X 被删除，随后又创建了同名同命名空间的 Pod Y。
	// 此时 X 也需要处理，因为它可能是最后一个持有 PodGroup finalizer 的活跃 Pod。
	if newPod != nil && oldPod != nil && oldPod.UID != newPod.UID {
		c.enqueuePodGroupForPod(logger, oldPod)
	}

	if newPod == nil && oldPod != nil {
		c.enqueuePodGroupForPod(logger, oldPod)
	}
}

// enqueuePodGroupForPod 将 Pod 引用的 PodGroup 加入队列。
// 调用方负责保证只传入状态变化可能导致 finalizer 被移除的 Pod（已删除或已进入终止阶段）。
func (c *Controller) enqueuePodGroupForPod(logger klog.Logger, pod *v1.Pod) {
	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return
	}

	pgKey := pod.Namespace + "/" + *pod.Spec.SchedulingGroup.PodGroupName
	logger.V(4).Info("Enqueuing PodGroup for pod event", "pod", klog.KObj(pod), "podGroup", pgKey)
	c.queue.Add(pgKey)
}

// getPod 从 informer 事件对象中解析出 Pod，兼容删除事件中的 tombstone 包装。
func getPod(obj interface{}) *v1.Pod {
	if obj == nil {
		return nil
	}
	pod, ok := obj.(*v1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return nil
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Pod %#v", obj))
			return nil
		}
	}
	return pod
}
