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

// Package deployment 包含处理 Kubernetes Deployment 的全部逻辑。
// 它实现了部署应用的一组策略（滚动更新、重建），回滚到历史版本的能力，
// 用于降低风险的比例扩缩容、清理策略，以及 Deployment 的其他实用特性。
package deployment

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/deployment/util"
)

const (
	// maxRetries 是 Deployment 被丢弃出队列前重试的次数。
	// 按当前使用的限速器（5ms*2^(maxRetries-1)），以下为 Deployment
	// 被重新入队的时间间隔：
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s, 5.1s, 10.2s, 20.4s, 41s, 82s
	maxRetries = 15
)

// controllerKind 保存该控制器类型对应的 schema.GroupVersionKind。
var controllerKind = apps.SchemeGroupVersion.WithKind("Deployment")

// DeploymentController 负责把系统中存储的 Deployment 对象与
// 实际运行的 ReplicaSet 和 Pod 保持同步。
type DeploymentController struct {
	// rsControl 用于认领/释放 ReplicaSet。
	rsControl controller.RSControlInterface
	client    clientset.Interface

	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	// 允许在测试中注入 syncDeployment。
	syncHandler func(ctx context.Context, dKey string) error
	// 用于单元测试。
	enqueueDeployment func(deployment *apps.Deployment)

	// dLister 可从共享 informer 的存储中列举/获取 Deployment。
	dLister appslisters.DeploymentLister
	// rsLister 可从共享 informer 的存储中列举/获取 ReplicaSet。
	rsLister appslisters.ReplicaSetLister
	// podLister 可从共享 informer 的存储中列举/获取 Pod。
	podLister corelisters.PodLister
	// podIndexer 支持按 ControllerRef 的 UID 查找 Pod。
	podIndexer cache.Indexer

	// dListerSynced 在 Deployment 存储至少同步过一次后返回 true。
	// 作为结构体成员是为了方便在测试中注入。
	dListerSynced cache.InformerSynced
	// rsListerSynced 在 ReplicaSet 存储至少同步过一次后返回 true。
	// 作为结构体成员是为了方便在测试中注入。
	rsListerSynced cache.InformerSynced
	// podListerSynced 在 Pod 存储至少同步过一次后返回 true。
	// 作为结构体成员是为了方便在测试中注入。
	podListerSynced cache.InformerSynced

	// 需要被同步的 Deployment。
	queue workqueue.TypedRateLimitingInterface[string]
}

// NewDeploymentController 创建一个新的 DeploymentController，
// 注册 Deployment、ReplicaSet、Pod 的事件处理器并建立 Pod 的 ControllerRef 索引。
func NewDeploymentController(ctx context.Context, dInformer appsinformers.DeploymentInformer, rsInformer appsinformers.ReplicaSetInformer, podInformer coreinformers.PodInformer, client clientset.Interface) (*DeploymentController, error) {
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	logger := klog.FromContext(ctx)
	dc := &DeploymentController{
		client:           client,
		eventBroadcaster: eventBroadcaster,
		eventRecorder:    eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "deployment-controller"}),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "deployment",
			},
		),
	}
	dc.rsControl = controller.RealRSControl{
		KubeClient: client,
		Recorder:   dc.eventRecorder,
	}

	dInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dc.addDeployment(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dc.updateDeployment(logger, oldObj, newObj)
		},
		// 删除事件仍会进入同步循环，但因对象已从存储中删除而不会产生实际动作。
		DeleteFunc: func(obj interface{}) {
			dc.deleteDeployment(logger, obj)
		},
	})
	rsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dc.addReplicaSet(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dc.updateReplicaSet(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			dc.deleteReplicaSet(logger, obj)
		},
	})
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			dc.deletePod(logger, obj)
		},
	})

	dc.syncHandler = dc.syncDeployment
	dc.enqueueDeployment = dc.enqueue

	dc.dLister = dInformer.Lister()
	dc.rsLister = rsInformer.Lister()
	dc.podLister = podInformer.Lister()
	dc.dListerSynced = dInformer.Informer().HasSynced
	dc.rsListerSynced = rsInformer.Informer().HasSynced
	dc.podListerSynced = podInformer.Informer().HasSynced

	if err := controller.AddPodControllerIndexer(podInformer.Informer()); err != nil {
		return nil, fmt.Errorf("adding Pod controller UID indexer: %w", err)
	}
	dc.podIndexer = podInformer.Informer().GetIndexer()

	return dc, nil
}

// Run 启动控制器的监视与同步循环，直到 ctx 结束。
func (dc *DeploymentController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	// 启动事件处理流水线。
	dc.eventBroadcaster.StartStructuredLogging(3)
	dc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: dc.client.CoreV1().Events("")})
	defer dc.eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting controller", "controller", "deployment")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down controller", "controller", "deployment")
		dc.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, dc.dListerSynced, dc.rsListerSynced, dc.podListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, dc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// addDeployment 在 Deployment 创建时将其加入同步队列。
func (dc *DeploymentController) addDeployment(logger klog.Logger, obj interface{}) {
	d := obj.(*apps.Deployment)
	logger.V(4).Info("Adding deployment", "deployment", klog.KObj(d))
	dc.enqueueDeployment(d)
}

// updateDeployment 在 Deployment 更新时将最新版本加入同步队列。
func (dc *DeploymentController) updateDeployment(logger klog.Logger, old, cur interface{}) {
	oldD := old.(*apps.Deployment)
	curD := cur.(*apps.Deployment)
	logger.V(4).Info("Updating deployment", "deployment", klog.KObj(oldD))
	dc.enqueueDeployment(curD)
}

// deleteDeployment 在 Deployment 删除时将其加入同步队列，兼容墓碑对象。
func (dc *DeploymentController) deleteDeployment(logger klog.Logger, obj interface{}) {
	d, ok := obj.(*apps.Deployment)
	// 删除事件可能丢失，此时 informer 会投递墓碑对象；需从中取出原始 Deployment。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		d, ok = tombstone.Obj.(*apps.Deployment)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a Deployment %#v", obj))
			return
		}
	}
	logger.V(4).Info("Deleting deployment", "deployment", klog.KObj(d))
	dc.enqueueDeployment(d)
}

// addReplicaSet 在 ReplicaSet 创建时，把管理它的 Deployment 加入同步队列。
func (dc *DeploymentController) addReplicaSet(logger klog.Logger, obj interface{}) {
	rs := obj.(*apps.ReplicaSet)

	if rs.DeletionTimestamp != nil {
		// 控制器管理器重启后，对象可能一出现就处于待删除状态，按删除事件处理。
		dc.deleteReplicaSet(logger, rs)
		return
	}
	// 若存在 ControllerRef，则归属明确，直接找对应的 Deployment。
	if controllerRef := metav1.GetControllerOf(rs); controllerRef != nil {
		d := dc.resolveControllerRef(rs.Namespace, controllerRef)
		if d == nil {
			return
		}
		logger.V(4).Info("ReplicaSet added", "replicaSet", klog.KObj(rs))
		dc.enqueueDeployment(d)
		return
	}

	// 否则是孤儿 ReplicaSet：找出所有标签匹配的 Deployment 并同步，
	// 让它们有机会认领该对象。
	ds := dc.getDeploymentsForReplicaSet(logger, rs)
	if len(ds) == 0 {
		return
	}
	logger.V(4).Info("Orphan ReplicaSet added", "replicaSet", klog.KObj(rs))
	for _, d := range ds {
		dc.enqueueDeployment(d)
	}
}

// getDeploymentsForReplicaSet 返回所有可能匹配该 ReplicaSet 的 Deployment。
func (dc *DeploymentController) getDeploymentsForReplicaSet(logger klog.Logger, rs *apps.ReplicaSet) []*apps.Deployment {
	deployments, err := util.GetDeploymentsForReplicaSet(dc.dLister, rs)
	if err != nil || len(deployments) == 0 {
		return nil
	}
	// 同一 Deployment 下的所有 ReplicaSet 都带有唯一标签键，正常情况下
	// 上面的查询最多返回一个 Deployment。若返回多个，理想做法是动态修复
	// （清理其中一个控制器），目前先沿用第一个（最早的）结果。
	if len(deployments) > 1 {
		// ControllerRef 会防止做出危险操作，但列表里出现多个对象仍属于用户配置错误。
		logger.V(4).Info("user error! more than one deployment is selecting replica set",
			"replicaSet", klog.KObj(rs), "labels", rs.Labels, "deployment", klog.KObj(deployments[0]))
	}
	return deployments
}

// updateReplicaSet 在 ReplicaSet 更新时，找出管理它的 Deployment 并唤醒它们。
// 当归属或标签发生变化时，新旧关联的 Deployment 都需要同步。
// old 和 cur 必须是 *apps.ReplicaSet 类型。
func (dc *DeploymentController) updateReplicaSet(logger klog.Logger, old, cur interface{}) {
	curRS := cur.(*apps.ReplicaSet)
	oldRS := old.(*apps.ReplicaSet)
	if curRS.ResourceVersion == oldRS.ResourceVersion {
		// 周期性 resync 会对所有已知的 ReplicaSet 发送更新事件；
		// 真正变化过的对象其 ResourceVersion 一定不同，因此可直接忽略。
		return
	}

	curControllerRef := metav1.GetControllerOf(curRS)
	oldControllerRef := metav1.GetControllerOf(oldRS)
	controllerRefChanged := !reflect.DeepEqual(curControllerRef, oldControllerRef)
	if controllerRefChanged && oldControllerRef != nil {
		// ControllerRef 发生变化时，也要同步旧控制器（如果存在），
		// 让它有机会释放该 ReplicaSet。
		if d := dc.resolveControllerRef(oldRS.Namespace, oldControllerRef); d != nil {
			dc.enqueueDeployment(d)
		}
	}
	// 当前对象有 ControllerRef 时归属明确，只需同步它的新控制器。
	if curControllerRef != nil {
		d := dc.resolveControllerRef(curRS.Namespace, curControllerRef)
		if d == nil {
			return
		}
		logger.V(4).Info("ReplicaSet updated", "replicaSet", klog.KObj(curRS))
		dc.enqueueDeployment(d)
		return
	}

	// 否则是孤儿：若标签或归属发生变化，就同步所有匹配的 Deployment，
	// 看是否有人现在愿意认领它。
	labelChanged := !reflect.DeepEqual(curRS.Labels, oldRS.Labels)
	if labelChanged || controllerRefChanged {
		ds := dc.getDeploymentsForReplicaSet(logger, curRS)
		if len(ds) == 0 {
			return
		}
		logger.V(4).Info("Orphan ReplicaSet updated", "replicaSet", klog.KObj(curRS))
		for _, d := range ds {
			dc.enqueueDeployment(d)
		}
	}
}

// deleteReplicaSet 在 ReplicaSet 删除时，把管理它的 Deployment 加入同步队列。
// obj 可能是 *apps.ReplicaSet，也可能是 DeletedFinalStateUnknown 标记对象。
func (dc *DeploymentController) deleteReplicaSet(logger klog.Logger, obj interface{}) {
	rs, ok := obj.(*apps.ReplicaSet)

	// 删除事件可能丢失，relist 会发现存储中多出对象，从而插入包含被删键值
	// 的墓碑对象。注意墓碑内容可能已过期：若 ReplicaSet 改过标签，新的
	// Deployment 要等到周期性 resync 才会被唤醒。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		rs, ok = tombstone.Obj.(*apps.ReplicaSet)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a ReplicaSet %#v", obj))
			return
		}
	}

	controllerRef := metav1.GetControllerOf(rs)
	if controllerRef == nil {
		// 孤儿对象无人负责，删除时无需通知任何控制器。
		return
	}
	d := dc.resolveControllerRef(rs.Namespace, controllerRef)
	if d == nil {
		return
	}
	logger.V(4).Info("ReplicaSet deleted", "replicaSet", klog.KObj(rs))
	dc.enqueueDeployment(d)
}

// deletePod 在 Pod 被删除且该 Pod 属于 Recreate 策略的 Deployment 时，
// 将 Deployment 加入队列以推进重建流程。
func (dc *DeploymentController) deletePod(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)

	// 删除事件可能丢失，relist 会发现存储中多出 Pod，从而插入包含被删键值
	// 的墓碑对象。注意墓碑内容可能已过期：若 Pod 改过标签，新的 Deployment
	// 要等到周期性 resync 才会被唤醒。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %#v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a pod %#v", obj))
			return
		}
	}
	d := dc.getDeploymentForPod(logger, pod)
	if d == nil {
		return
	}
	logger.V(4).Info("Pod deleted", "pod", klog.KObj(pod))
	if d.Spec.Strategy.Type == apps.RecreateDeploymentStrategyType {
		dc.enqueueDeployment(d)
	}
}

// enqueue 根据 Deployment 的命名空间/名称生成键并加入同步队列。
func (dc *DeploymentController) enqueue(deployment *apps.Deployment) {
	key, err := controller.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", deployment, err))
		return
	}

	dc.queue.Add(key)
}

// enqueueRateLimited 按限速器的退避延迟把 Deployment 重新加入队列。
func (dc *DeploymentController) enqueueRateLimited(deployment *apps.Deployment) {
	key, err := controller.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", deployment, err))
		return
	}

	dc.queue.AddRateLimited(key)
}

// enqueueAfter 会在指定时长之后把 Deployment 加入队列。
func (dc *DeploymentController) enqueueAfter(deployment *apps.Deployment, after time.Duration) {
	key, err := controller.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", deployment, err))
		return
	}

	dc.queue.AddAfter(key, after)
}

// getDeploymentForPod 返回管理给定 Pod 的 Deployment。
func (dc *DeploymentController) getDeploymentForPod(logger klog.Logger, pod *v1.Pod) *apps.Deployment {
	// 先找到拥有该 Pod 的 ReplicaSet。
	var rs *apps.ReplicaSet
	var err error
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		// Pod 没有控制器所有者。
		return nil
	}
	if controllerRef.Kind != apps.SchemeGroupVersion.WithKind("ReplicaSet").Kind {
		// 该 Pod 不属于 ReplicaSet。
		return nil
	}
	rs, err = dc.rsLister.ReplicaSets(pod.Namespace).Get(controllerRef.Name)
	// 名称可能复用，必须同时校验 UID，避免认领到同名的旧对象。
	if err != nil || rs.UID != controllerRef.UID {
		logger.V(4).Info("Cannot get replicaset for pod", "ownerReference", controllerRef.Name, "pod", klog.KObj(pod), "err", err)
		return nil
	}

	// 再沿着 ControllerRef 找到拥有该 ReplicaSet 的 Deployment。
	controllerRef = metav1.GetControllerOf(rs)
	if controllerRef == nil {
		return nil
	}
	return dc.resolveControllerRef(rs.Namespace, controllerRef)
}

// resolveControllerRef 返回 ControllerRef 指向的控制器；若无法解析为
// 类型正确且匹配的控制器，则返回 nil。
func (dc *DeploymentController) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *apps.Deployment {
	// lister 不支持按 UID 查找，只能按名称查找后再校验 UID。
	// 类型不对时连名称查找都无需进行。
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	d, err := dc.dLister.Deployments(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if d.UID != controllerRef.UID {
		// 按名称找到的控制器并非 ControllerRef 指向的那个实例。
		return nil
	}
	return d
}

// worker 运行工作线程，不断出队、处理并标记任务完成。
// 队列保证同一个键不会被并发调用 syncHandler。
func (dc *DeploymentController) worker(ctx context.Context) {
	for dc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的下一项；队列关闭时返回 false。
func (dc *DeploymentController) processNextWorkItem(ctx context.Context) bool {
	key, quit := dc.queue.Get()
	if quit {
		return false
	}
	defer dc.queue.Done(key)

	err := dc.syncHandler(ctx, key)
	dc.handleErr(ctx, err, key)

	return true
}

// handleErr 根据错误类型决定重试、忽略还是丢弃该键。
func (dc *DeploymentController) handleErr(ctx context.Context, err error, key string) {
	logger := klog.FromContext(ctx)
	// 命名空间正在终止时不再重试，否则会一直失败并占用队列。
	if err == nil || errors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
		dc.queue.Forget(key)
		return
	}
	ns, name, keyErr := cache.SplitMetaNamespaceKey(key)
	if keyErr != nil {
		logger.Error(err, "Failed to split meta namespace cache key", "cacheKey", key)
	}

	if dc.queue.NumRequeues(key) < maxRetries {
		logger.V(2).Info("Error syncing deployment", "deployment", klog.KRef(ns, name), "err", err)
		dc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logger.V(2).Info("Dropping deployment out of the queue", "deployment", klog.KRef(ns, name), "err", err)
	dc.queue.Forget(key)
}

// getReplicaSetsForDeployment 通过 ControllerRefManager 以认领和孤立的方式
// 对 ControllerRef 进行调和，返回该 Deployment 应当管理的 ReplicaSet 列表。
func (dc *DeploymentController) getReplicaSetsForDeployment(ctx context.Context, d *apps.Deployment) ([]*apps.ReplicaSet, error) {
	// 列举所有 ReplicaSet，找出归属于自己但已不再匹配选择器的那些；
	// ClaimReplicaSets() 会把它们变为孤立对象。
	rsList, err := dc.rsLister.ReplicaSets(d.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	deploymentSelector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("deployment %s/%s has invalid label selector: %v", d.Namespace, d.Name, err)
	}
	// 一旦要执行认领，就在列举 ReplicaSet 之后用绕过缓存的 quorum 读
	// 重新检查删除状态，避免认领一个已被删除的 Deployment（见 #42639）。
	canAdoptFunc := controller.RecheckDeletionTimestamp(func(ctx context.Context) (metav1.Object, error) {
		fresh, err := dc.client.AppsV1().Deployments(d.Namespace).Get(ctx, d.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		// 名称相同但 UID 不同说明原对象已被删除并重建，不能继续认领。
		if fresh.UID != d.UID {
			return nil, fmt.Errorf("original Deployment %v/%v is gone: got uid %v, wanted %v", d.Namespace, d.Name, fresh.UID, d.UID)
		}
		return fresh, nil
	})
	cm := controller.NewReplicaSetControllerRefManager(dc.rsControl, d, deploymentSelector, controllerKind, canAdoptFunc)
	return cm.ClaimReplicaSets(ctx, rsList)
}

// getPodMapForDeployment 返回由 Deployment 管理的 Pod。
//
// 它根据 Pod 的 ControllerRef，返回一个从 ReplicaSet UID 到该 RS 控制的
// Pod 列表的映射。
// 注意：返回的 Pod 指针直接指向缓存中的对象，绝不能对其进行修改。
func (dc *DeploymentController) getPodMapForDeployment(d *apps.Deployment, rsList []*apps.ReplicaSet) (map[types.UID][]*v1.Pod, error) {
	// 按 Pod 的控制器（若该控制器在 rsList 中）对 Pod 分组。
	podMap := make(map[types.UID][]*v1.Pod, len(rsList))

	for _, rs := range rsList {
		// 借助 Pod 索引器，只查找该 ReplicaSet 名下的 Pod。
		pods, err := controller.FilterPodsByOwner(dc.podIndexer, &rs.ObjectMeta, "ReplicaSet", false)
		if err != nil {
			return nil, err
		}
		podMap[rs.UID] = pods
	}
	return podMap, nil
}

// syncDeployment 同步给定键对应的 Deployment。
// 同一个键不应并发调用该函数。
func (dc *DeploymentController) syncDeployment(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logger.Error(err, "Failed to split meta namespace cache key", "cacheKey", key)
		return err
	}

	startTime := time.Now()
	logger.V(4).Info("Started syncing deployment", "deployment", klog.KRef(namespace, name), "startTime", startTime)
	defer func() {
		logger.V(4).Info("Finished syncing deployment", "deployment", klog.KRef(namespace, name), "duration", time.Since(startTime))
	}()

	deployment, err := dc.dLister.Deployments(namespace).Get(name)
	if errors.IsNotFound(err) {
		logger.V(2).Info("Deployment has been deleted", "deployment", klog.KRef(namespace, name))
		return nil
	}
	if err != nil {
		return err
	}

	// 必须深拷贝，否则后续修改会直接改动缓存中的对象。
	d := deployment.DeepCopy()

	everything := metav1.LabelSelector{}
	if reflect.DeepEqual(d.Spec.Selector, &everything) {
		dc.eventRecorder.Eventf(d, v1.EventTypeWarning, "SelectingAll", "This deployment is selecting all pods. A non-empty selector is required.")
		if d.Status.ObservedGeneration < d.Generation {
			d.Status.ObservedGeneration = d.Generation
			dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
		}
		return nil
	}

	// 列举归属于该 Deployment 的 ReplicaSet，同时通过认领/孤立调和 ControllerRef。
	rsList, err := dc.getReplicaSetsForDeployment(ctx, d)
	if err != nil {
		return err
	}

	if d.DeletionTimestamp != nil {
		return dc.syncStatusOnly(ctx, d, rsList)
	}

	// 暂停/恢复 Deployment 时，先把进度状态更新为 Unknown，
	// 这样用户恢复一个设置了 progressDeadlineSeconds 的 Deployment 时，
	// 才不会因计时从暂停期间算起而误判超时。
	if err = dc.checkPausedConditions(ctx, d); err != nil {
		return err
	}

	if d.Spec.Paused {
		return dc.sync(ctx, d, rsList)
	}

	// 若底层 ReplicaSet 被更新到新版本，回滚操作不是可重入的。
	// 因此在后续入队中确认 Deployment 已清理 rollback 规格之前，
	// 不能继续更新 ReplicaSet。
	if getRollbackTo(d) != nil {
		return dc.rollback(ctx, d, rsList)
	}

	scalingEvent, err := dc.isScalingEvent(ctx, d, rsList)
	if err != nil {
		return err
	}
	if scalingEvent {
		return dc.sync(ctx, d, rsList)
	}

	switch d.Spec.Strategy.Type {
	case apps.RecreateDeploymentStrategyType:
		// 列举该 Deployment 名下所有 Pod 并按 ReplicaSet 分组，
		// 以确认 Recreate 发布过程中没有旧 Pod 仍在运行。
		podMap, err := dc.getPodMapForDeployment(d, rsList)
		if err != nil {
			return err
		}
		return dc.rolloutRecreate(ctx, d, rsList, podMap)
	case apps.RollingUpdateDeploymentStrategyType:
		return dc.rolloutRolling(ctx, d, rsList)
	}
	return fmt.Errorf("unexpected deployment strategy type: %s", d.Spec.Strategy.Type)
}
