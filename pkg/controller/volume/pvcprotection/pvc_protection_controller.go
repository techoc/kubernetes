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

package pvcprotection

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/util/protectionutil"
	"k8s.io/kubernetes/pkg/controller/volume/common"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/util/slice"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// LazyLivePodList 延迟查询并缓存实时 Pod 列表，使同一命名空间的多次 PVC 检查复用结果。
type LazyLivePodList struct {
	// cache 保存首次实时列表查询得到的 Pod 列表，nil 表示尚未查询过。
	cache []v1.Pod
	// controller 用于在需要发起实时列表时访问客户端。
	controller *Controller
}

// getCache 返回已缓存的实时 Pod 列表。
func (ll *LazyLivePodList) getCache() []v1.Pod {
	return ll.cache
}

// setCache 缓存一次实时 Pod 列表查询的结果。
func (ll *LazyLivePodList) setCache(pods []v1.Pod) {
	ll.cache = pods
}

// pvcData 记录一个待处理 PVC 的队列键及其名称。
type pvcData struct {
	pvcKey  string
	pvcName string
}

// pvcProcessingStore 按命名空间聚合待处理的 PVC，并按命名空间逐个处理，
// 以便同一命名空间内的多次检查复用同一份实时 Pod 列表。
type pvcProcessingStore struct {
	namespaceToPVCsMap map[string][]pvcData
	namespaceQueue     workqueue.TypedInterface[string]
	mu                 sync.Mutex
}

// NewPVCProcessingStore 创建一个空的 PVC 处理存储。
func NewPVCProcessingStore() *pvcProcessingStore {
	return &pvcProcessingStore{
		namespaceToPVCsMap: make(map[string][]pvcData),
		namespaceQueue:     workqueue.NewTyped[string](),
	}
}

// addOrUpdate 把指定命名空间下的 PVC 加入待处理集合。
func (m *pvcProcessingStore) addOrUpdate(namespace string, pvcKey, pvcName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 只有该命名空间首次出现时才入队，避免同一命名空间的多个 PVC 被重复调度。
	if _, exists := m.namespaceToPVCsMap[namespace]; !exists {
		m.namespaceToPVCsMap[namespace] = make([]pvcData, 0)
		m.namespaceQueue.Add(namespace)
	}
	m.namespaceToPVCsMap[namespace] = append(m.namespaceToPVCsMap[namespace], pvcData{pvcKey: pvcKey, pvcName: pvcName})
}

// flushNextPVCsByNamespace 返回下一个待处理命名空间及其关联的 PVC 列表，供下游处理。
func (m *pvcProcessingStore) flushNextPVCsByNamespace() ([]pvcData, string) {

	nextNamespace, quit := m.namespaceQueue.Get()
	// 队列关闭时 Get 返回 quit=true，表示没有更多命名空间需要处理。
	if quit {
		return nil, nextNamespace
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	pvcs := m.namespaceToPVCsMap[nextNamespace]

	delete(m.namespaceToPVCsMap, nextNamespace)
	m.namespaceQueue.Done(nextNamespace)
	return pvcs, nextNamespace
}

// Controller 监听 PVC 与 Pod 事件，负责为 PVC 添加/移除保护 finalizer，
// 并在启用相应特性时维护 PVC 的 Unused 状态条件。
type Controller struct {
	client clientset.Interface

	pvcLister       corelisters.PersistentVolumeClaimLister
	pvcListerSynced cache.InformerSynced

	podLister       corelisters.PodLister
	podListerSynced cache.InformerSynced
	podIndexer      cache.Indexer

	queue              workqueue.TypedRateLimitingInterface[string]
	pvcProcessingStore *pvcProcessingStore
}

// unusedSinceNowFunc 保存"当前时间"的获取函数，测试可将其替换为固定时间以验证状态更新时间。
var unusedSinceNowFunc = metav1.Now

// podUsageCheckFunc 判断给定 Pod 是否在使用指定 PVC；删除保护与 Unused 状态跟踪的判定规则不同。
type podUsageCheckFunc func(logger klog.Logger, pod *v1.Pod, pvc *v1.PersistentVolumeClaim) bool

// NewPVCProtectionController 创建 PVCProtectionController 新实例。
func NewPVCProtectionController(logger klog.Logger, pvcInformer coreinformers.PersistentVolumeClaimInformer, podInformer coreinformers.PodInformer, cl clientset.Interface) (*Controller, error) {
	e := &Controller{
		client: cl,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "pvcprotection"},
		),
		pvcProcessingStore: NewPVCProcessingStore(),
	}

	e.pvcLister = pvcInformer.Lister()
	e.pvcListerSynced = pvcInformer.Informer().HasSynced
	_, err := pvcInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e.pvcAddedUpdated(logger, obj)
		},
		UpdateFunc: func(old, new interface{}) {
			e.pvcAddedUpdated(logger, new)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add PVC event handler: %w", err)
	}

	e.podLister = podInformer.Lister()
	e.podListerSynced = podInformer.Informer().HasSynced
	e.podIndexer = podInformer.Informer().GetIndexer()
	// 通过 PVC 索引快速找出可能使用该 PVC 的 Pod，避免每次遍历全部 Pod。
	if err := common.AddIndexerIfNotPresent(e.podIndexer, common.PodPVCIndex, common.PodPVCIndexFunc()); err != nil {
		return nil, fmt.Errorf("could not initialize pvc protection controller: %w", err)
	}
	_, err = podInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e.podAddedDeletedUpdated(logger, nil, obj, false)
		},
		DeleteFunc: func(obj interface{}) {
			e.podAddedDeletedUpdated(logger, nil, obj, true)
		},
		UpdateFunc: func(old, new interface{}) {
			e.podAddedDeletedUpdated(logger, old, new, false)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return nil, fmt.Errorf("could not add Pod event handler: %w", err)
	}

	return e, nil
}

// Run 运行控制器的各个 goroutine，直到 ctx 结束。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting PVC protection controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down PVC protection controller")
		// 关闭两个工作队列并等待所有 worker 退出，避免退出时丢失正在处理的项。
		c.queue.ShutDown()
		c.pvcProcessingStore.namespaceQueue.ShutDown()
		wg.Wait()
	}()

	// 必须等缓存同步完成，否则基于 lister 的判断会遗漏尚未同步的对象。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.pvcListerSynced, c.podListerSynced) {
		return
	}

	wg.Go(func() {
		wait.UntilWithContext(ctx, c.runMainWorker, time.Second)
	})
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runProcessNamespaceWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runMainWorker 批量从 Informer 工作队列拉取 PVC 项，填充命名空间队列和命名空间-PVC 映射。
func (c *Controller) runMainWorker(ctx context.Context) {
	for c.processNextWorkItem() {
	}
}

// runProcessNamespaceWorker 从命名空间队列取出项，并处理该命名空间关联的 PVC。
func (c *Controller) runProcessNamespaceWorker(ctx context.Context) {
	for c.processPVCsByNamespace(ctx) {
	}
}

// processNextWorkItem 从工作队列取出一个 PVC 键，加入按命名空间聚合的处理存储。
func (c *Controller) processNextWorkItem() bool {
	pvcKey, quit := c.queue.Get()
	if quit {
		return false
	}

	pvcNamespace, pvcName, err := cache.SplitMetaNamespaceKey(pvcKey)
	if err != nil {
		// 键格式非法时无法定位 PVC，记录错误后跳过该项。
		utilruntime.HandleError(fmt.Errorf("error parsing PVC key %q: %w", pvcKey, err))
		return true
	}

	c.pvcProcessingStore.addOrUpdate(pvcNamespace, pvcKey, pvcName)
	return true
}

// processPVCsByNamespace 处理一个命名空间下的所有 PVC，并根据处理结果决定重试或忘记对应的队列项。
func (c *Controller) processPVCsByNamespace(ctx context.Context) bool {
	pvcList, namespace := c.pvcProcessingStore.flushNextPVCsByNamespace()
	if pvcList == nil {
		return false
	}

	// 同一批 PVC 共享一份实时 Pod 列表，减少 API 请求次数。
	lazyLivePodList := &LazyLivePodList{controller: c}
	for _, item := range pvcList {
		pvcKey, pvcName := item.pvcKey, item.pvcName
		err := c.processPVC(ctx, namespace, pvcName, lazyLivePodList)
		if err == nil {
			c.queue.Forget(pvcKey)
		} else {
			// 处理失败时限流重试，避免错误项占满队列。
			c.queue.AddRateLimited(pvcKey)
			utilruntime.HandleError(fmt.Errorf("PVC %v/%v failed with: %w", pvcName, namespace, err))
		}
		c.queue.Done(pvcKey)
	}
	return true
}

// processPVC 处理单个 PVC：按需更新 Unused 条件、维护保护 finalizer。
func (c *Controller) processPVC(ctx context.Context, pvcNamespace, pvcName string, lazyLivePodList *LazyLivePodList) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Processing PVC", "PVC", klog.KRef(pvcNamespace, pvcName))
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished processing PVC", "PVC", klog.KRef(pvcNamespace, pvcName), "duration", time.Since(startTime))
	}()

	pvc, err := c.pvcLister.PersistentVolumeClaims(pvcNamespace).Get(pvcName)
	if apierrors.IsNotFound(err) {
		// PVC 已被删除，无需再做任何处理。
		logger.V(4).Info("PVC not found, ignoring", "PVC", klog.KRef(pvcNamespace, pvcName))
		return nil
	}
	if err != nil {
		return err
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.PersistentVolumeClaimUnusedSinceTime) &&
		pvc.DeletionTimestamp == nil && pvc.Status.Phase == v1.ClaimBound {
		// 跳过正在删除或尚未绑定的 PVC，避免与 PV 控制器的绑定流程冲突。
		isUsed, err := c.isBeingUsedWith(ctx, pvc, lazyLivePodList, c.podUsesPVCForUnusedSince)
		if err != nil {
			return err
		}
		if err := c.updateUnusedCondition(ctx, pvc, isUsed); err != nil {
			return err
		}
	}

	if protectionutil.IsDeletionCandidate(pvc, volumeutil.PVCProtectionFinalizer) {
		// PVC 正处于删除流程：检查是否仍被使用，不再使用则移除 finalizer。
		isUsed, err := c.isBeingUsedWith(ctx, pvc, lazyLivePodList, c.podUsesPVCForDeletion)
		if err != nil {
			return err
		}
		if !isUsed {
			return c.removeFinalizer(ctx, pvc)
		}
		logger.V(2).Info("Keeping PVC because it is being used", "PVC", klog.KObj(pvc))
	}

	if protectionutil.NeedToAddFinalizer(pvc, volumeutil.PVCProtectionFinalizer) {
		// PVC 未进入删除流程，应当带有 finalizer。该 finalizer 通常由准入插件添加，
		// 这里仅为兼容在准入插件启用之前创建的旧 PVC 而补加。
		return c.addFinalizer(ctx, pvc)
	}
	return nil
}

// addFinalizer 为 PVC 添加保护 finalizer。
func (c *Controller) addFinalizer(ctx context.Context, pvc *v1.PersistentVolumeClaim) error {
	// 操作副本而非缓存中的对象，避免污染 Informer 缓存。
	claimClone := pvc.DeepCopy()
	claimClone.ObjectMeta.Finalizers = append(claimClone.ObjectMeta.Finalizers, volumeutil.PVCProtectionFinalizer)
	_, err := c.client.CoreV1().PersistentVolumeClaims(claimClone.Namespace).Update(ctx, claimClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Error adding protection finalizer to PVC", "PVC", klog.KObj(pvc))
		return err
	}
	logger.V(3).Info("Added protection finalizer to PVC", "PVC", klog.KObj(pvc))
	return nil
}

// removeFinalizer 从 PVC 移除保护 finalizer，使其可以被真正删除。
func (c *Controller) removeFinalizer(ctx context.Context, pvc *v1.PersistentVolumeClaim) error {
	claimClone := pvc.DeepCopy()
	claimClone.ObjectMeta.Finalizers = slice.RemoveString(claimClone.ObjectMeta.Finalizers, volumeutil.PVCProtectionFinalizer, nil)
	_, err := c.client.CoreV1().PersistentVolumeClaims(claimClone.Namespace).Update(ctx, claimClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Error removing protection finalizer from PVC", "PVC", klog.KObj(pvc))
		return err
	}
	logger.V(3).Info("Removed protection finalizer from PVC", "PVC", klog.KObj(pvc))
	return nil
}

// updateUnusedCondition 根据 PVC 是否仍被使用更新其 Unused 状态条件，状态未变化时不做写入。
func (c *Controller) updateUnusedCondition(ctx context.Context, pvc *v1.PersistentVolumeClaim, isUsed bool) error {
	existingCondition := findCondition(pvc.Status.Conditions, v1.PersistentVolumeClaimUnused)

	switch {
	case isUsed && (existingCondition == nil || existingCondition.Status == v1.ConditionTrue):
		// PVC 之前未被使用而现在被使用 -> 置 Unused=False。
		return c.setUnusedCondition(ctx, pvc, v1.ConditionFalse, "PodUsingPVC", "A pod is currently referencing this PVC")
	case !isUsed && (existingCondition == nil || existingCondition.Status != v1.ConditionTrue):
		// PVC 未被使用且现有状态未反映这一点 -> 置 Unused=True。
		return c.setUnusedCondition(ctx, pvc, v1.ConditionTrue, "NoPodsUsingPVC", "No pods are currently referencing this PVC")
	default:
		// 状态已经正确，避免无谓的状态更新请求。
		return nil
	}
}

// setUnusedCondition 将 PVC 状态中的 Unused 条件更新为给定状态。
func (c *Controller) setUnusedCondition(ctx context.Context, pvc *v1.PersistentVolumeClaim, status v1.ConditionStatus, reason, message string) error {
	claimClone := pvc.DeepCopy()
	now := unusedSinceNowFunc()
	newCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimUnused,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}

	// 状态条件列表最多只保留一个 Unused 条件，存在时原地替换。
	if existing := findCondition(claimClone.Status.Conditions, v1.PersistentVolumeClaimUnused); existing != nil {
		*existing = newCondition
	} else {
		claimClone.Status.Conditions = append(claimClone.Status.Conditions, newCondition)
	}

	_, err := c.client.CoreV1().PersistentVolumeClaims(claimClone.Namespace).UpdateStatus(ctx, claimClone, metav1.UpdateOptions{})
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Error updating Unused condition in PVC status", "PVC", klog.KObj(pvc))
		return err
	}
	logger.V(3).Info("Updated Unused condition in PVC status", "PVC", klog.KObj(pvc), "status", status)
	return nil
}

// findCondition 在条件列表中查找指定类型的条件，不存在时返回 nil。
func findCondition(conditions []v1.PersistentVolumeClaimCondition, condType v1.PersistentVolumeClaimConditionType) *v1.PersistentVolumeClaimCondition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// isBeingUsedWith 使用给定的 Pod 判定函数检查 PVC 是否仍被占用。
func (c *Controller) isBeingUsedWith(ctx context.Context, pvc *v1.PersistentVolumeClaim, lazyLivePodList *LazyLivePodList, podUsage podUsageCheckFunc) (bool, error) {
	// 先查 Informer 缓存：若发现正在使用该 PVC 的 Pod，即可直接决定保留 PVC，
	// 无需发起开销较大的实时列表请求。
	logger := klog.FromContext(ctx)
	if inUse, err := c.askInformer(logger, pvc, podUsage); err != nil {
		// 缓存查询失败不必返回错误，因为后续还会通过实时列表再次确认。
		logger.Error(err, "")
	} else if inUse {
		return true, nil
	}

	// 缓存中没有找到使用者并不代表该 Pod 不存在，它可能只是尚未同步到缓存。
	// 要完全确认删除 PVC 是安全的，必须基于"惰性"实时列表再检查一次。

	// 惰性实时列表只缓存第一次成功查询的结果：短时间内大量 PVC 删除可复用该列表，
	// 避免向 API Server 发起大量重复请求。缓存会在每轮 processNextWorkItem() 中刷新。
	return c.askAPIServer(ctx, pvc, lazyLivePodList, podUsage)
}

// askInformer 在 Informer 缓存中查找使用指定 PVC 的 Pod。
func (c *Controller) askInformer(logger klog.Logger, pvc *v1.PersistentVolumeClaim, podUsage podUsageCheckFunc) (bool, error) {
	logger.V(4).Info("Looking for Pods using PVC in the Informer's cache", "PVC", klog.KObj(pvc))

	// 借助索引器找出可能使用该 PVC 的 Pod。
	objs, err := c.podIndexer.ByIndex(common.PodPVCIndex, fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
	if err != nil {
		return false, fmt.Errorf("cache-based list of pods failed while processing %s/%s: %s", pvc.Namespace, pvc.Name, err.Error())
	}
	for _, obj := range objs {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			continue
		}

		// 仍需逐个检查卷：对普通 PVC 卷这是冗余判断，但对临时卷（Ephemeral）
		// 必须确认该 PVC 正是为该 Pod 创建的那个。
		if podUsage(logger, pod, pvc) {
			return true, nil
		}
	}

	logger.V(4).Info("No Pod using PVC was found in the Informer's cache", "PVC", klog.KObj(pvc))
	return false, nil
}

// askAPIServer 通过实时列表查询 API Server，确认是否存在使用指定 PVC 的 Pod。
func (c *Controller) askAPIServer(ctx context.Context, pvc *v1.PersistentVolumeClaim, lazyLivePodList *LazyLivePodList, podUsage podUsageCheckFunc) (bool, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Looking for Pods using PVC", "PVC", klog.KObj(pvc))
	if lazyLivePodList.getCache() == nil {
		logger.V(4).Info("Live listing Pods in namespace", "namespace", pvc.Namespace)
		podsList, err := c.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, fmt.Errorf("live list of pods failed: %s", err.Error())
		}

		// 显式缓存空切片，避免 nil 被误判为"尚未查询"而重复发起请求。
		if podsList.Items == nil {
			lazyLivePodList.setCache(make([]v1.Pod, 0))
		} else {
			lazyLivePodList.setCache(podsList.Items)
		}
	}

	for _, pod := range lazyLivePodList.getCache() {
		if podUsage(logger, &pod, pvc) {
			return true, nil
		}
	}

	logger.V(2).Info("PVC is unused", "PVC", klog.KObj(pvc))
	return false, nil
}

// podUsesPVCForDeletion 判断 Pod 是否占用 PVC（用于删除保护）。
func (c *Controller) podUsesPVCForDeletion(logger klog.Logger, pod *v1.Pod, pvc *v1.PersistentVolumeClaim) bool {
	// 只有已调度的 Pod 才需要检查：kubelet 在 Pod 调度后才会处理它，且不允许
	// 启动引用带有非空 deletionTimestamp 的 PVC 的 Pod。
	if pod.Spec.NodeName != "" {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name ||
				!podIsShutDown(pod) && volume.Ephemeral != nil && ephemeral.VolumeClaimName(pod, &volume) == pvc.Name && ephemeral.VolumeIsForPod(pod, pvc) == nil {
				logger.V(2).Info("Pod uses PVC", "pod", klog.KObj(pod), "PVC", klog.KObj(pvc))
				return true
			}
		}
	}
	return false
}

// podUsesPVCForUnusedSince 判断 Pod 是否引用 PVC（用于维护 Unused 状态）。
func (c *Controller) podUsesPVCForUnusedSince(logger klog.Logger, pod *v1.Pod, pvc *v1.PersistentVolumeClaim) bool {
	// 已终止的 Pod 不再占用 PVC。
	if volumeutil.IsPodTerminated(pod, pod.Status) {
		return false
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name ||
			volume.Ephemeral != nil && ephemeral.VolumeClaimName(pod, &volume) == pvc.Name && ephemeral.VolumeIsForPod(pod, pvc) == nil {
			logger.V(4).Info("Pod references PVC for unused tracking", "pod", klog.KObj(pod), "PVC", klog.KObj(pvc))
			return true
		}
	}
	return false
}

// podIsShutDown 判断 kubelet 是否已经处理完该 Pod，或该 Pod 是否被强制删除。
func podIsShutDown(pod *v1.Pod) bool {
	// 带有 deletionTimestamp 且 deletionGracePeriodSeconds 为零的 Pod：
	// a) 已被 kubelet 处理并由 apiserver 设置了删除状态
	//    - canBeDeleted 已确认卷被卸载
	//      https://github.com/kubernetes/kubernetes/blob/5404b5a28a2114299608bab00e4292960dd864a0/pkg/kubelet/kubelet_pods.go#L980
	//    - deletionGracePeriodSeconds 是通过零宽限期删除设置的
	//      https://github.com/kubernetes/kubernetes/blob/5404b5a28a2114299608bab00e4292960dd864a0/pkg/kubelet/status/status_manager.go#L580-L592
	// 或
	// b) 被强制删除。
	//
	// 此时它只是在等待垃圾回收。理论上可以等待它真正消失，但 Pod 的 finalizer
	// 可能阻塞删除，从而造成延迟。
	//
	// 更糟的是可能存在循环依赖（Pod 的 finalizer 等待 PVC 被删除，而 PVC 保护
	// 控制器又在等待 Pod 被删除）。此时视 PVC 为未被使用，可打破这种循环。
	//
	// 因此继续删除 PVC 是更好的选择：对情况 a) 是安全的，对情况 b) 也是期望的行为。
	return pod.DeletionTimestamp != nil && pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds == 0
}

// pvcAddedUpdated 响应 PVC 添加/更新事件。
func (c *Controller) pvcAddedUpdated(logger klog.Logger, obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PVC informer returned non-PVC object: %#v", obj))
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pvc)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for Persistent Volume Claim %#v: %v", pvc, err))
		return
	}
	logger.V(4).Info("Got event on PVC", "pvc", klog.KObj(pvc))

	// 新 PVC 尚无 Unused 条件，需要入队以初始化该条件。
	if utilfeature.DefaultFeatureGate.Enabled(features.PersistentVolumeClaimUnusedSinceTime) && findCondition(pvc.Status.Conditions, v1.PersistentVolumeClaimUnused) == nil {
		c.queue.Add(key)
	}

	if protectionutil.NeedToAddFinalizer(pvc, volumeutil.PVCProtectionFinalizer) || protectionutil.IsDeletionCandidate(pvc, volumeutil.PVCProtectionFinalizer) {
		c.queue.Add(key)
	}
}

// podAddedDeletedUpdated 响应 Pod 的添加、更新和删除事件。
func (c *Controller) podAddedDeletedUpdated(logger klog.Logger, old, new interface{}, deleted bool) {
	if pod := c.parsePod(new); pod != nil {
		c.enqueuePVCs(logger, pod, deleted)

		// 一次更新通知可能掩盖了如下过程：Pod X 被删除，随后又创建了同名同命名空间的
		// Pod Y。此时仍需处理 X，否则当 X 阻塞了 Y 未引用的 PVC 删除时，该 PVC
		// 将永远无法被删除。
		if oldPod := c.parsePod(old); oldPod != nil && oldPod.UID != pod.UID {
			c.enqueuePVCs(logger, oldPod, true)
		}
	}
}

// parsePod 从事件对象中解析出 Pod，兼容删除事件中的 tombstone 对象。
func (*Controller) parsePod(obj interface{}) *v1.Pod {
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

// enqueuePVCs 把 Pod 使用的所有 PVC 加入工作队列。
func (c *Controller) enqueuePVCs(logger klog.Logger, pod *v1.Pod, deleted bool) {
	// 过滤掉对移除 PVC finalizer 没有帮助的 Pod：未删除、未终止且已调度的 Pod
	// 对删除保护没有影响；启用 Unused 状态特性时则需跟踪所有 Pod。
	if !utilfeature.DefaultFeatureGate.Enabled(features.PersistentVolumeClaimUnusedSinceTime) &&
		!deleted && !volumeutil.IsPodTerminated(pod, pod.Status) && pod.Spec.NodeName != "" {
		return
	}

	logger.V(4).Info("Enqueuing PVCs for Pod", "pod", klog.KObj(pod), "podUID", pod.UID)

	// 将该 Pod 引用的所有 PVC 入队。
	for _, volume := range pod.Spec.Volumes {
		switch {
		case volume.PersistentVolumeClaim != nil:
			c.queue.Add(pod.Namespace + "/" + volume.PersistentVolumeClaim.ClaimName)
		case volume.Ephemeral != nil:
			c.queue.Add(pod.Namespace + "/" + ephemeral.VolumeClaimName(pod, &volume))
		}
	}
}
