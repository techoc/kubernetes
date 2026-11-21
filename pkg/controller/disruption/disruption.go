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

package disruption

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	apps "k8s.io/api/apps/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	policy "k8s.io/api/policy/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	policyinformers "k8s.io/client-go/informers/policy/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	pdbhelper "k8s.io/component-helpers/apps/poddisruptionbudget"
	"k8s.io/klog/v2"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/clock"
)

const (
	// DeletionTimeout 规定 Pod 被写入 PDB.Status.DisruptedPods 之后，
	// 最多允许多长时间才被本控制器观察到 DeletionTimestamp。
	// 超时仍未删除就认为它不会被删除，可以从 pdb.Status.DisruptedPods 中移除该条目。
	// 这里假设 apiserver 到控制器的延迟很小（1-2 秒），因此两分钟足够宽裕。
	// 若控制器与驱逐方不在同一节点，必须保证两边时钟同步（例如通过 ntp），
	// 否则 PodDisruptionBudget 可能无法提供足够保护，误放行非预期中断。
	DeletionTimeout = 2 * time.Minute

	// stalePodDisruptionTimeout 规定 DisruptionTarget 条件可以"陈旧"多久：
	// 条件存在但 Pod 没有 DeletionTimestamp。超过该时长后本控制器会尝试把条件置为 False。
	stalePodDisruptionTimeout = 2 * time.Minute
)

// updater 定义写回 PodDisruptionBudget 状态的函数签名。
// 通过注入不同实现，测试可以在不调用真实 API 的情况下校验同步逻辑。
type updater func(context.Context, *policy.PodDisruptionBudget) error

// DisruptionController 负责维护 PodDisruptionBudget 的状态：统计匹配 Pod 的健康数、
// 计算允许的中断数、清理过期的 DisruptedPods 条目，并重置陈旧的 DisruptionTarget 条件。
type DisruptionController struct {
	kubeClient clientset.Interface
	mapper     apimeta.RESTMapper

	scaleNamespacer scaleclient.ScalesGetter
	discoveryClient discovery.DiscoveryInterfaceWithContext

	pdbLister       policylisters.PodDisruptionBudgetLister
	pdbListerSynced cache.InformerSynced

	podLister       corelisters.PodLister
	podListerSynced cache.InformerSynced

	rcLister       corelisters.ReplicationControllerLister
	rcListerSynced cache.InformerSynced

	rsLister       appsv1listers.ReplicaSetLister
	rsListerSynced cache.InformerSynced

	dLister       appsv1listers.DeploymentLister
	dListerSynced cache.InformerSynced

	ssLister       appsv1listers.StatefulSetLister
	ssListerSynced cache.InformerSynced

	// 待同步的 PodDisruptionBudget 键；限速队列保证失败重试有退避且不重复堆积。
	queue        workqueue.TypedRateLimitingInterface[string]
	recheckQueue workqueue.TypedDelayingInterface[string]

	// 因 DisruptionTarget 条件陈旧而待清理的 Pod 键，与主队列分离以免相互阻塞。
	stalePodDisruptionQueue   workqueue.TypedRateLimitingInterface[string]
	stalePodDisruptionTimeout time.Duration

	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder

	getUpdater func() updater

	clock clock.Clock
}

// controllerAndScale 承载 controller finder 返回的（控制器 UID, 副本数）对。
// 调用方只关心规模，不需要知道具体是哪种工作负载资源。
type controllerAndScale struct {
	types.UID
	scale int32
}

// podControllerFinder 把 Pod 的属主引用映射为对应控制器及其规模。
// 返回 nil 表示当前 finder 不适用，调用方会继续尝试下一个；返回 error 表示输入异常。
type podControllerFinder func(ctx context.Context, controllerRef *metav1.OwnerReference, namespace string) (*controllerAndScale, error)

// NewDisruptionController 构造生产环境使用的 DisruptionController。
// 使用真实时钟和默认超时；测试应改用 NewDisruptionControllerInternal 注入可控时钟。
func NewDisruptionController(
	ctx context.Context,
	podInformer coreinformers.PodInformer,
	pdbInformer policyinformers.PodDisruptionBudgetInformer,
	rcInformer coreinformers.ReplicationControllerInformer,
	rsInformer appsv1informers.ReplicaSetInformer,
	dInformer appsv1informers.DeploymentInformer,
	ssInformer appsv1informers.StatefulSetInformer,
	kubeClient clientset.Interface,
	restMapper apimeta.RESTMapper,
	scaleNamespacer scaleclient.ScalesGetter,
	discoveryClient discovery.DiscoveryInterface,
) *DisruptionController {
	return NewDisruptionControllerInternal(
		ctx,
		podInformer,
		pdbInformer,
		rcInformer,
		rsInformer,
		dInformer,
		ssInformer,
		kubeClient,
		restMapper,
		scaleNamespacer,
		discoveryClient,
		clock.RealClock{},
		stalePodDisruptionTimeout)
}

// NewDisruptionControllerInternal 构造 DisruptionController，并允许注入 clock 与
// stalePodDisruptionTimeout。仅用于测试：注入假时钟可以在不真等两分钟的情况下
// 验证 DisruptedPods 过期和 DisruptionTarget 条件清理逻辑。
func NewDisruptionControllerInternal(ctx context.Context,
	podInformer coreinformers.PodInformer,
	pdbInformer policyinformers.PodDisruptionBudgetInformer,
	rcInformer coreinformers.ReplicationControllerInformer,
	rsInformer appsv1informers.ReplicaSetInformer,
	dInformer appsv1informers.DeploymentInformer,
	ssInformer appsv1informers.StatefulSetInformer,
	kubeClient clientset.Interface,
	restMapper apimeta.RESTMapper,
	scaleNamespacer scaleclient.ScalesGetter,
	discoveryClient discovery.DiscoveryInterface,
	clock clock.WithTicker,
	stalePodDisruptionTimeout time.Duration,
) *DisruptionController {
	logger := klog.FromContext(ctx)
	dc := &DisruptionController{
		kubeClient: kubeClient,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				DelayingQueue: workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{
					Logger: &logger,
					Clock:  clock,
					Name:   "disruption",
				}),
			},
		),
		recheckQueue: workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{
			Logger: &logger,
			Clock:  clock,
			Name:   "disruption_recheck",
		}),
		stalePodDisruptionQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				DelayingQueue: workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{
					Logger: &logger,
					Clock:  clock,
					Name:   "stale_pod_disruption",
				}),
			},
		),
		broadcaster:               record.NewBroadcaster(record.WithContext(ctx)),
		stalePodDisruptionTimeout: stalePodDisruptionTimeout,
	}
	dc.recorder = dc.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "controllermanager"})

	// 默认通过 writePdbStatus 写回 API；测试可替换 getUpdater 以拦截状态更新。
	dc.getUpdater = func() updater { return dc.writePdbStatus }

	// Pod 事件只入队其匹配到的 PDB，避免为无关 Pod 做全量重算。
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dc.addPod(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dc.updatePod(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			dc.deletePod(logger, obj)
		},
	})
	dc.podLister = podInformer.Lister()
	dc.podListerSynced = podInformer.Informer().HasSynced

	// PDB 自身变更直接入队，与 Pod 事件共用同一限速和去重逻辑。
	pdbInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			dc.addDB(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			dc.updateDB(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			dc.removeDB(logger, obj)
		},
	})
	dc.pdbLister = pdbInformer.Lister()
	dc.pdbListerSynced = pdbInformer.Informer().HasSynced

	dc.rcLister = rcInformer.Lister()
	dc.rcListerSynced = rcInformer.Informer().HasSynced

	dc.rsLister = rsInformer.Lister()
	dc.rsListerSynced = rsInformer.Informer().HasSynced

	dc.dLister = dInformer.Lister()
	dc.dListerSynced = dInformer.Informer().HasSynced

	dc.ssLister = ssInformer.Lister()
	dc.ssListerSynced = ssInformer.Informer().HasSynced

	dc.mapper = restMapper
	dc.scaleNamespacer = scaleNamespacer
	dc.discoveryClient = discovery.ToDiscoveryInterfaceWithContext(discoveryClient)

	dc.clock = clock

	return dc
}

// finders 返回按优先级排列的控制器查找器。
// 工作负载资源都实现了 scale 子资源，理论上只查 scale 即可；但 scale 无法利用 lister
// 的本地缓存，因此优先直接查工作负载对象，只有都不匹配时才回退到 scale 子资源。
func (dc *DisruptionController) finders() []podControllerFinder {
	return []podControllerFinder{
		dc.getPodReplicationController, dc.getPodDeployment, dc.getPodReplicaSet,
		dc.getPodStatefulSet, dc.getScaleController,
	}
}

var (
	controllerKindRS  = v1beta1.SchemeGroupVersion.WithKind("ReplicaSet")
	controllerKindSS  = apps.SchemeGroupVersion.WithKind("StatefulSet")
	controllerKindRC  = v1.SchemeGroupVersion.WithKind("ReplicationController")
	controllerKindDep = v1beta1.SchemeGroupVersion.WithKind("Deployment")
)

// getPodReplicaSet 返回未被 Deployment 接管的 ReplicaSet 及其副本数。
// 若 RS 已归属 Deployment，则由 getPodDeployment 统计，避免同一 Pod 被重复计数。
func (dc *DisruptionController) getPodReplicaSet(ctx context.Context, controllerRef *metav1.OwnerReference, namespace string) (*controllerAndScale, error) {
	ok, err := verifyGroupKind(controllerRef, controllerKindRS.Kind, []string{"apps", "extensions"})
	if !ok || err != nil {
		return nil, err
	}
	rs, err := dc.rsLister.ReplicaSets(namespace).Get(controllerRef.Name)
	if err != nil {
		// lister 里唯一可能的错误是 NotFound，表示对象已消失，这里视为不适用。
		return nil, nil
	}
	if rs.UID != controllerRef.UID {
		// UID 不同说明是同名新建对象，旧属主引用已经失效。
		return nil, nil
	}
	controllerRef = metav1.GetControllerOf(rs)
	if controllerRef != nil && controllerRef.Kind == controllerKindDep.Kind {
		// RS 受 Deployment 管理时，规模应记到 Deployment 上，否则会重复计算。
		return nil, nil
	}
	return &controllerAndScale{rs.UID, *(rs.Spec.Replicas)}, nil
}

// getPodStatefulSet 返回 controllerRef 指向的 StatefulSet 及其期望副本数。
func (dc *DisruptionController) getPodStatefulSet(ctx context.Context, controllerRef *metav1.OwnerReference, namespace string) (*controllerAndScale, error) {
	ok, err := verifyGroupKind(controllerRef, controllerKindSS.Kind, []string{"apps"})
	if !ok || err != nil {
		return nil, err
	}
	ss, err := dc.ssLister.StatefulSets(namespace).Get(controllerRef.Name)
	if err != nil {
		// NotFound 表示对象已被删除，属主引用自然失效，不构成同步错误。
		return nil, nil
	}
	if ss.UID != controllerRef.UID {
		return nil, nil
	}

	return &controllerAndScale{ss.UID, *(ss.Spec.Replicas)}, nil
}

// getPodDeployment 沿 Pod -> ReplicaSet -> Deployment 的属主链查找管理该 Pod 的 Deployment。
// 每一步都校验 UID，防止同名对象重建后把新旧对象错误关联起来。
func (dc *DisruptionController) getPodDeployment(ctx context.Context, controllerRef *metav1.OwnerReference, namespace string) (*controllerAndScale, error) {
	ok, err := verifyGroupKind(controllerRef, controllerKindRS.Kind, []string{"apps", "extensions"})
	if !ok || err != nil {
		return nil, err
	}
	rs, err := dc.rsLister.ReplicaSets(namespace).Get(controllerRef.Name)
	if err != nil {
		// The only possible error is NotFound, which is ok here.
		return nil, nil
	}
	if rs.UID != controllerRef.UID {
		return nil, nil
	}
	controllerRef = metav1.GetControllerOf(rs)
	if controllerRef == nil {
		return nil, nil
	}

	ok, err = verifyGroupKind(controllerRef, controllerKindDep.Kind, []string{"apps", "extensions"})
	if !ok || err != nil {
		return nil, err
	}
	deployment, err := dc.dLister.Deployments(rs.Namespace).Get(controllerRef.Name)
	if err != nil {
		// NotFound 说明 Deployment 已删除，此时该 Pod 不应再计入其规模。
		return nil, nil
	}
	if deployment.UID != controllerRef.UID {
		return nil, nil
	}
	return &controllerAndScale{deployment.UID, *(deployment.Spec.Replicas)}, nil
}

// getPodReplicationController 返回 controllerRef 指向的 ReplicationController 及其副本数。
func (dc *DisruptionController) getPodReplicationController(ctx context.Context, controllerRef *metav1.OwnerReference, namespace string) (*controllerAndScale, error) {
	ok, err := verifyGroupKind(controllerRef, controllerKindRC.Kind, []string{""})
	if !ok || err != nil {
		return nil, err
	}
	rc, err := dc.rcLister.ReplicationControllers(namespace).Get(controllerRef.Name)
	if err != nil {
		// NotFound 表示 RC 已不存在，不能据此推断 Pod 的期望规模。
		return nil, nil
	}
	if rc.UID != controllerRef.UID {
		return nil, nil
	}
	return &controllerAndScale{rc.UID, *(rc.Spec.Replicas)}, nil
}

// getScaleController 是兜底查找器：先用 RESTMapper 把 GroupKind 映射到资源，
// 再读取该资源的 scale 子资源。仅在所有内置工作负载 finder 都不适用时使用。
func (dc *DisruptionController) getScaleController(ctx context.Context, controllerRef *metav1.OwnerReference, namespace string) (*controllerAndScale, error) {
	gv, err := schema.ParseGroupVersion(controllerRef.APIVersion)
	if err != nil {
		return nil, err
	}

	gk := schema.GroupKind{
		Group: gv.Group,
		Kind:  controllerRef.Kind,
	}

	mapping, err := dc.mapper.RESTMapping(gk, gv.Version)
	if err != nil {
		return nil, err
	}
	gr := mapping.Resource.GroupResource()
	scale, err := dc.scaleNamespacer.Scales(namespace).Get(ctx, gr, controllerRef.Name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// NotFound 既可能是资源本身不存在，也可能是资源存在但没实现 scale 子资源。
			// 查一次 discovery 区分两种情况，才能给出准确的诊断信息。
			isScale, err := dc.implementsScale(ctx, mapping.Resource)
			if err != nil {
				return nil, err
			}
			if !isScale {
				return nil, fmt.Errorf("%s does not implement the scale subresource", gr.String())
			}
			return nil, nil
		}
		return nil, err
	}
	if scale.UID != controllerRef.UID {
		return nil, nil
	}
	return &controllerAndScale{scale.UID, scale.Spec.Replicas}, nil
}

// implementsScale 通过 discovery 判断指定 GVR 是否暴露了受支持的 scale 子资源。
// 仅匹配子资源名字还不够，还要确认其 group/version/kind 与 scale converter 支持的版本一致。
func (dc *DisruptionController) implementsScale(ctx context.Context, gvr schema.GroupVersionResource) (bool, error) {
	resourceList, err := dc.discoveryClient.ServerResourcesForGroupVersionWithContext(ctx, gvr.GroupVersion().String())
	if err != nil {
		return false, err
	}

	scaleSubresourceName := fmt.Sprintf("%s/scale", gvr.Resource)
	for _, resource := range resourceList.APIResources {
		if resource.Name != scaleSubresourceName {
			continue
		}

		for _, scaleGv := range scaleclient.NewScaleConverter().ScaleVersions() {
			if resource.Group == scaleGv.Group &&
				resource.Version == scaleGv.Version &&
				resource.Kind == "Scale" {
				return true, nil
			}
		}
	}
	return false, nil
}

// verifyGroupKind 校验属主引用的 Kind 与 API group 是否符合期望。
// APIVersion 解析失败属于异常输入，必须向上返回错误，不能静默当作不匹配。
func verifyGroupKind(controllerRef *metav1.OwnerReference, expectedKind string, expectedGroups []string) (bool, error) {
	gv, err := schema.ParseGroupVersion(controllerRef.APIVersion)
	if err != nil {
		return false, err
	}

	if controllerRef.Kind != expectedKind {
		return false, nil
	}

	return slices.Contains(expectedGroups, gv.Group), nil
}

// Run 启动事件广播与 informer 缓存同步，随后拉起三类 worker：PDB 同步、延迟复查、
// 过期 DisruptionTarget 清理。ctx 取消后关闭所有队列并等待 worker 退出。
func (dc *DisruptionController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	// Start events processing pipeline.
	if dc.kubeClient != nil {
		logger.Info("Sending events to api server.")
		dc.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: dc.kubeClient.CoreV1().Events("")})
	} else {
		logger.Info("No api server defined - no events will be sent to API server.")
	}
	defer dc.broadcaster.Shutdown()

	logger.Info("Starting disruption controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down disruption controller")
		dc.queue.ShutDown()
		dc.recheckQueue.ShutDown()
		dc.stalePodDisruptionQueue.ShutDown()
		wg.Wait()
	}()

	// 缓存未同步前绝不启动 worker：否则 lister 返回空集合会算出错误的允许中断数。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, dc.podListerSynced, dc.pdbListerSynced, dc.rcListerSynced, dc.rsListerSynced, dc.dListerSynced, dc.ssListerSynced) {
		return
	}

	for range workers {
		wg.Go(func() {
			wait.UntilWithContext(ctx, dc.worker, time.Second)
		})
	}
	// 复查队列只负责到期后把键转回主队列，不直接做状态计算。
	wg.Go(func() {
		wait.Until(dc.recheckWorker, time.Second, ctx.Done())
	})
	wg.Go(func() {
		wait.UntilWithContext(ctx, dc.stalePodDisruptionWorker, time.Second)
	})
	<-ctx.Done()
}

// addDB 处理 PDB 新增事件：把对象入队，等待 sync 计算其状态。
func (dc *DisruptionController) addDB(logger klog.Logger, obj interface{}) {
	pdb := obj.(*policy.PodDisruptionBudget)
	logger.V(4).Info("Add DB", "podDisruptionBudget", klog.KObj(pdb))
	dc.enqueuePdb(logger, pdb)
}

// updateDB 处理 PDB 更新事件：不比较新旧对象，统一入队重算，
// 因为 spec 和 status 的任意变化都可能影响允许中断数。
func (dc *DisruptionController) updateDB(logger klog.Logger, old, cur interface{}) {
	pdb := cur.(*policy.PodDisruptionBudget)
	logger.V(4).Info("Update DB", "podDisruptionBudget", klog.KObj(pdb))
	dc.enqueuePdb(logger, pdb)
}

// removeDB 处理 PDB 删除事件：删除可能以 tombstone 形式到达，需要先解包。
// 这里不做存在性检查，由 sync 阶段通过 NotFound 自然收敛。
func (dc *DisruptionController) removeDB(logger klog.Logger, obj interface{}) {
	pdb, ok := obj.(*policy.PodDisruptionBudget)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			logger.Error(nil, "Couldn't get object from tombstone", "obj", obj)
			return
		}
		pdb, ok = tombstone.Obj.(*policy.PodDisruptionBudget)
		if !ok {
			logger.Error(nil, "Tombstone contained object that is not a PDB", "obj", obj)
			return
		}
	}
	logger.V(4).Info("Remove DB", "podDisruptionBudget", klog.KObj(pdb))
	dc.enqueuePdb(logger, pdb)
}

// addPod 处理 Pod 新增事件：只入队其匹配的 PDB，避免为无关 Pod 做全量重算；
// 同时检查是否需要安排过期 DisruptionTarget 条件的清理。
func (dc *DisruptionController) addPod(logger klog.Logger, obj interface{}) {
	pod := obj.(*v1.Pod)
	logger.V(4).Info("AddPod called on pod", "pod", klog.KObj(pod))
	pdb := dc.getPdbForPod(logger, pod)
	if pdb == nil {
		logger.V(4).Info("No matching PDB for pod", "pod", klog.KObj(pod))
	} else {
		logger.V(4).Info("addPod -> PDB", "pod", klog.KObj(pod), "podDisruptionBudget", klog.KObj(pdb))
		dc.enqueuePdb(logger, pdb)
	}
	if has, cleanAfter := dc.nonTerminatingPodHasStaleDisruptionCondition(pod); has {
		dc.enqueueStalePodDisruptionCleanup(logger, pod, cleanAfter)
	}
}

// updatePod 处理 Pod 更新事件：标签或 Ready 状态变化都可能改变 PDB 的健康数，
// 因此按当前对象重新查找匹配 PDB 并入队。
func (dc *DisruptionController) updatePod(logger klog.Logger, _, cur interface{}) {
	pod := cur.(*v1.Pod)
	logger.V(4).Info("UpdatePod called on pod", "pod", klog.KObj(pod))
	pdb := dc.getPdbForPod(logger, pod)
	if pdb == nil {
		logger.V(4).Info("No matching PDB for pod", "pod", klog.KObj(pod))
	} else {
		logger.V(4).Info("updatePod -> PDB", "pod", klog.KObj(pod), "podDisruptionBudget", klog.KObj(pdb))
		dc.enqueuePdb(logger, pdb)
	}
	if has, cleanAfter := dc.nonTerminatingPodHasStaleDisruptionCondition(pod); has {
		dc.enqueueStalePodDisruptionCleanup(logger, pod, cleanAfter)
	}
}

// deletePod 处理 Pod 删除事件：重新入队匹配的 PDB，使 currentHealthy 及时回落。
func (dc *DisruptionController) deletePod(logger klog.Logger, obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	// 删除事件丢失时，relist 会发现 store 中存在而列表里没有的 Pod，
	// 从而插入包含已删除 key/value 的 tombstone。该值可能是旧的：若 Pod 改过标签，
	// 新的 ReplicaSet 要等到周期性 resync 才会被唤醒。
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			logger.Error(nil, "Couldn't get object from tombstone", "obj", obj)
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			logger.Error(nil, "Tombstone contained object that is not a pod", "obj", obj)
			return
		}
	}
	logger.V(4).Info("DeletePod called on pod", "pod", klog.KObj(pod))
	pdb := dc.getPdbForPod(logger, pod)
	if pdb == nil {
		logger.V(4).Info("No matching PDB for pod", "pod", klog.KObj(pod))
		return
	}
	logger.V(4).Info("DeletePod -> PDB", "pod", klog.KObj(pod), "podDisruptionBudget", klog.KObj(pdb))
	dc.enqueuePdb(logger, pdb)
}

// enqueuePdb 把 PDB 的命名空间键加入主工作队列；限速队列负责去重和失败退避。
func (dc *DisruptionController) enqueuePdb(logger klog.Logger, pdb *policy.PodDisruptionBudget) {
	key, err := controller.KeyFunc(pdb)
	if err != nil {
		logger.Error(err, "Couldn't get key for PodDisruptionBudget", "podDisruptionBudget", klog.KObj(pdb))
		return
	}
	dc.queue.Add(key)
}

// enqueuePdbForRecheck 延迟重新入队 PDB，用于在未来某个时刻复核
// DisruptedPods 中记录的 Pod 是否真的被删除。
func (dc *DisruptionController) enqueuePdbForRecheck(logger klog.Logger, pdb *policy.PodDisruptionBudget, delay time.Duration) {
	key, err := controller.KeyFunc(pdb)
	if err != nil {
		logger.Error(err, "Couldn't get key for PodDisruptionBudget", "podDisruptionBudget", klog.KObj(pdb))
		return
	}
	dc.recheckQueue.AddAfter(key, delay)
}

// enqueueStalePodDisruptionCleanup 按剩余超时时间延迟入队 Pod；
// 条件刚变旧时先等待剩余时间，避免过早清掉仍然有效的信号。
func (dc *DisruptionController) enqueueStalePodDisruptionCleanup(logger klog.Logger, pod *v1.Pod, d time.Duration) {
	key, err := controller.KeyFunc(pod)
	if err != nil {
		logger.Error(err, "Couldn't get key for Pod object", "pod", klog.KObj(pod))
		return
	}
	dc.stalePodDisruptionQueue.AddAfter(key, d)
	logger.V(4).Info("Enqueued pod to cleanup stale DisruptionTarget condition", "pod", klog.KObj(pod))
}

// getPdbForPod 返回匹配该 Pod 的 PDB；没有匹配项属于正常情况，返回 nil 即可。
// 一个 Pod 命中多个 PDB 时任意选一个并告警：Selector 重叠是用户配置问题，
// 控制器不应因此停摆。
func (dc *DisruptionController) getPdbForPod(logger klog.Logger, pod *v1.Pod) *policy.PodDisruptionBudget {
	// GetPodPodDisruptionBudgets 只在找不到 PDB 时返回错误，这里把"没有匹配"当作正常情况处理。
	pdbs, err := dc.pdbLister.GetPodPodDisruptionBudgets(pod)
	if err != nil {
		logger.V(4).Info("No PodDisruptionBudgets found for pod, PodDisruptionBudget controller will avoid syncing.", "pod", klog.KObj(pod))
		return nil
	}

	if len(pdbs) > 1 {
		msg := fmt.Sprintf("Pod %q/%q matches multiple PodDisruptionBudgets.  Chose %q arbitrarily.", pod.Namespace, pod.Name, pdbs[0].Name)
		logger.Info(msg)
		dc.recorder.Event(pod, v1.EventTypeWarning, "MultiplePodDisruptionBudgets", msg)
	}
	return pdbs[0]
}

// getPodsForPdb 按 PDB 的 selector 列出同命名空间下的 Pod。
// 返回的 Pod 来自共享 lister，调用方绝不能修改，否则会污染其他控制器的缓存视图。
func (dc *DisruptionController) getPodsForPdb(pdb *policy.PodDisruptionBudget) ([]*v1.Pod, error) {
	sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return []*v1.Pod{}, err
	}
	pods, err := dc.podLister.Pods(pdb.Namespace).List(sel)
	if err != nil {
		return []*v1.Pod{}, err
	}
	return pods, nil
}

// worker 持续消费主工作队列，直到队列关闭或 ctx 被取消。
func (dc *DisruptionController) worker(ctx context.Context) {
	for dc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 同步一个 PDB 键：成功则 Forget，失败则记录错误并按限速重新入队，
// 避免单个坏对象造成无限热循环或拖垮整个 worker。
func (dc *DisruptionController) processNextWorkItem(ctx context.Context) bool {
	dKey, quit := dc.queue.Get()
	if quit {
		return false
	}
	defer dc.queue.Done(dKey)

	err := dc.sync(ctx, dKey)
	if err == nil {
		dc.queue.Forget(dKey)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, err, "Error syncing PodDisruptionBudget, requeuing", "key", dKey)
	dc.queue.AddRateLimited(dKey)

	return true
}

// recheckWorker 持续消费延迟复查队列，只做"到期后重新入主队列"这一件事。
func (dc *DisruptionController) recheckWorker() {
	for dc.processNextRecheckWorkItem() {
	}
}

// processNextRecheckWorkItem 到期后把键转到主队列，让常规 sync 流程重新计算 PDB 状态。
func (dc *DisruptionController) processNextRecheckWorkItem() bool {
	dKey, quit := dc.recheckQueue.Get()
	if quit {
		return false
	}
	defer dc.recheckQueue.Done(dKey)
	dc.queue.AddRateLimited(dKey)
	return true
}

// stalePodDisruptionWorker 持续消费过期条件清理队列，直到 ctx 取消。
func (dc *DisruptionController) stalePodDisruptionWorker(ctx context.Context) {
	for dc.processNextStalePodDisruptionWorkItem(ctx) {
	}
}

// processNextStalePodDisruptionWorkItem 清理单个 Pod 的过期 DisruptionTarget 条件；
// 失败时限速重试，避免一次 API 冲突导致条件永久残留。
func (dc *DisruptionController) processNextStalePodDisruptionWorkItem(ctx context.Context) bool {
	key, quit := dc.stalePodDisruptionQueue.Get()
	if quit {
		return false
	}
	defer dc.stalePodDisruptionQueue.Done(key)
	err := dc.syncStalePodDisruption(ctx, key)
	if err == nil {
		dc.stalePodDisruptionQueue.Forget(key)
		return true
	}
	utilruntime.HandleError(fmt.Errorf("error syncing Pod %v to clear DisruptionTarget condition, requeueing: %w", key, err))
	dc.stalePodDisruptionQueue.AddRateLimited(key)
	return true
}

// sync 从缓存取出 PDB 并尝试同步；对象已删除视为同步成功。
// 冲突错误直接交给队列重试，其他错误则先走 failSafe 把 DisruptionsAllowed 置 0，
// 确保在状态算不准时 /evict 不会误放行驱逐。
func (dc *DisruptionController) sync(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := dc.clock.Now()
	defer func() {
		logger.V(4).Info("Finished syncing PodDisruptionBudget", "key", key, "duration", dc.clock.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pdb, err := dc.pdbLister.PodDisruptionBudgets(namespace).Get(name)
	if errors.IsNotFound(err) {
		logger.V(4).Info("podDisruptionBudget has been deleted", "key", key)
		return nil
	}
	if err != nil {
		return err
	}

	err = dc.trySync(ctx, pdb)
	// 冲突说明刚有别的写入者更新过该 PDB，直接返回让队列重试即可；
	// 此时触发 failSafe 反而会把允许中断数错误地清零。
	if errors.IsConflict(err) {
		return err
	}
	if err != nil {
		logger.Error(err, "Failed to sync PDB", "podDisruptionBudget", klog.KRef(namespace, name))
		return dc.failSafe(ctx, pdb, err)
	}

	return nil
}

// trySync 完成一次完整的状态计算：选 Pod、算期望规模、构建 DisruptedPods 映射、
// 统计健康副本并写回状态，最后按最早到期时间安排复查。
func (dc *DisruptionController) trySync(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	logger := klog.FromContext(ctx)
	pods, err := dc.getPodsForPdb(pdb)
	if err != nil {
		dc.recorder.Eventf(pdb, v1.EventTypeWarning, "NoPods", "Failed to get pods: %v", err)
		return err
	}
	if len(pods) == 0 {
		dc.recorder.Eventf(pdb, v1.EventTypeNormal, "NoPods", "No matching pods found")
	}

	expectedCount, desiredHealthy, unmanagedPods, err := dc.getExpectedPodCount(ctx, pdb, pods)
	if err != nil {
		dc.recorder.Eventf(pdb, v1.EventTypeWarning, "CalculateExpectedPodCountFailed", "Failed to calculate the number of expected pods: %v", err)
		return err
	}
	// 存在无控制器的 Pod 时，百分比形式的 minAvailable 无法精确计算；
	// 这里选择记录事件并继续，而不是返回错误造成热循环和频繁写 etcd。
	if len(unmanagedPods) > 0 {
		logger.V(4).Info("Found unmanaged pods associated with this PDB", "pods", unmanagedPods)
		dc.recorder.Eventf(pdb, v1.EventTypeWarning, "UnmanagedPods", "Pods selected by this PodDisruptionBudget (selector: %v) were found "+
			"to be unmanaged. As a result, the status of the PDB cannot be calculated correctly, which may result in undefined behavior. "+
			"To account for these pods please set \".spec.minAvailable\" "+
			"field of the PDB to an integer value.", pdb.Spec.Selector)
	}

	currentTime := dc.clock.Now()
	disruptedPods, recheckTime := dc.buildDisruptedPodMap(logger, pods, pdb, currentTime)
	currentHealthy := countHealthyPods(pods, disruptedPods, currentTime)
	err = dc.updatePdbStatus(ctx, pdb, currentHealthy, desiredHealthy, expectedCount, disruptedPods)

	if err == nil && recheckTime != nil {
		// 队列对同名 PDB 最多保留一条记录，且其关联的是最早那次入队提供的时间戳；
		// 因此用最早到期时间计算延迟，就能保证不会漏掉任何一次到期复查。
		dc.enqueuePdbForRecheck(logger, pdb, recheckTime.Sub(currentTime))
	}
	return err
}

// syncStalePodDisruption 把已过期的 DisruptionTarget 条件重置为 False。
// Pod 已删除或条件尚未超时会提前返回；未到期则按剩余时间重新排队等待。
func (dc *DisruptionController) syncStalePodDisruption(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := dc.clock.Now()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	defer func() {
		logger.V(4).Info("Finished syncing Pod to clear DisruptionTarget condition", "pod", klog.KRef(namespace, name), "duration", dc.clock.Since(startTime))
	}()
	pod, err := dc.podLister.Pods(namespace).Get(name)
	if errors.IsNotFound(err) {
		logger.V(4).Info("Skipping clearing DisruptionTarget condition because pod was deleted", "pod", klog.KObj(pod))
		return nil
	}
	if err != nil {
		return err
	}

	hasCond, cleanAfter := dc.nonTerminatingPodHasStaleDisruptionCondition(pod)
	if !hasCond {
		return nil
	}
	if cleanAfter > 0 {
		dc.enqueueStalePodDisruptionCleanup(logger, pod, cleanAfter)
		return nil
	}

	// 先深拷贝再修改，绝不直接改动 lister 缓存中的共享对象。
	newPod := pod.DeepCopy()
	updated := apipod.UpdatePodCondition(&newPod.Status, &v1.PodCondition{
		Type:               v1.DisruptionTarget,
		ObservedGeneration: apipod.CalculatePodConditionObservedGeneration(&newPod.Status, newPod.Generation, v1.DisruptionTarget),
		Status:             v1.ConditionFalse,
	})
	if !updated {
		return nil
	}
	if _, err := dc.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, newPod, metav1.UpdateOptions{}); err != nil {
		return err
	}
	logger.V(2).Info("Reset stale DisruptionTarget condition to False", "pod", klog.KObj(pod))
	return nil
}

// getExpectedPodCount 根据 maxUnavailable / minAvailable 计算期望 Pod 总数与期望健康数。
// 整数形式的 minAvailable 直接以匹配到的 Pod 数为基准，百分比形式则需要查询控制器规模。
func (dc *DisruptionController) getExpectedPodCount(ctx context.Context, pdb *policy.PodDisruptionBudget, pods []*v1.Pod) (expectedCount, desiredHealthy int32, unmanagedPods []string, err error) {
	err = nil
	// TODO(davidopp): 应把 expectedCount 的计算方式以及允许的控制器配置规则
	// （尤其是"被 PDB 覆盖的 Pod 有 0 个或超过 1 个控制器时报错"）在整数与百分比
	// 形式的 minAvailable 上统一处理，当前两种形式的语义并不一致。

	if pdb.Spec.MaxUnavailable != nil {
		expectedCount, unmanagedPods, err = dc.getExpectedScale(ctx, pods)
		if err != nil {
			return
		}
		var maxUnavailable int
		maxUnavailable, err = intstr.GetScaledValueFromIntOrPercent(pdb.Spec.MaxUnavailable, int(expectedCount), true)
		if err != nil {
			return
		}
		desiredHealthy = expectedCount - int32(maxUnavailable)
		if desiredHealthy < 0 {
			desiredHealthy = 0
		}
	} else if pdb.Spec.MinAvailable != nil {
		if pdb.Spec.MinAvailable.Type == intstr.Int {
			desiredHealthy = pdb.Spec.MinAvailable.IntVal
			expectedCount = int32(len(pods))
		} else if pdb.Spec.MinAvailable.Type == intstr.String {
			expectedCount, unmanagedPods, err = dc.getExpectedScale(ctx, pods)
			if err != nil {
				return
			}

			var minAvailable int
			minAvailable, err = intstr.GetScaledValueFromIntOrPercent(pdb.Spec.MinAvailable, int(expectedCount), true)
			if err != nil {
				return
			}
			desiredHealthy = int32(minAvailable)
		}
	}
	return
}

// getExpectedScale 以所有匹配 Pod 的控制器规模之和作为百分比计算的分母。
// 没有控制器的 Pod 被收集起来交给调用方告警；确实找不到控制器才视为异常并报错。
func (dc *DisruptionController) getExpectedScale(ctx context.Context, pods []*v1.Pod) (expectedCount int32, unmanagedPods []string, err error) {
	// 用户指定"可用 Pod 的比例"时，分母取
	// SUM_{all c in C} scale(c)
	// 其中 C 是 C_p1, C_p2, ..., C_pN 的并集，C_pi 是控制 Pod pi 的控制器集合。

	// Kubernetes 只定义了 Pod 被 0 或 1 个控制器管理时的行为：这里显式排除 0 个的情况，
	// 并在发现多于 1 个控制器时报错，因此实际上每个 C_pi 都恰好只有一个控制器。

	// 控制器 UID 到其规模的映射，按 UID 去重，避免同一控制器被多个 Pod 重复累加。
	controllerScale := map[types.UID]int32{}

	// 1. 为每个 Pod 找到控制器。

	// 现在允许通过 selector 把 PDB 应用到 Pod，因此可能存在没有后端控制器、
	// 但仍被 PDB 匹配的"无管理"Pod。这类 Pod 被收集上报而不是立刻抛同步错误，
	// 否则控制器会频繁更新 status 子资源，给 etcd 带来大量昂贵写操作。
	// 借助 ControllerRef，一个 Pod 最多只能有一个控制器。
	for _, pod := range pods {
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil {
			unmanagedPods = append(unmanagedPods, pod.Name)
			continue
		}

		// 同一控制器的规模已经记录过，无需重复查询 finder。
		if _, found := controllerScale[controllerRef.UID]; found {
			continue
		}

		// 依次尝试所有受支持的控制器类型，找到第一个匹配项即停止。
		foundController := false
		for _, finder := range dc.finders() {
			var controllerNScale *controllerAndScale
			controllerNScale, err = finder(ctx, controllerRef, pod.Namespace)
			if err != nil {
				return
			}
			if controllerNScale != nil {
				controllerScale[controllerNScale.UID] = controllerNScale.scale
				foundController = true
				break
			}
		}
		if !foundController {
			err = fmt.Errorf("found no controllers for pod %q", pod.Name)
			return
		}
	}

	// 2. 把所有控制器的规模相加，得到分母。
	expectedCount = 0
	for _, count := range controllerScale {
		expectedCount += count
	}

	return
}

// countHealthyPods 统计健康 Pod 数：正在删除的 Pod、以及已被记录为即将中断的 Pod 都不计入。
// 该值直接作为 DisruptionsAllowed 的分子，决定驱逐请求能否被放行。
func countHealthyPods(pods []*v1.Pod, disruptedPods map[string]metav1.Time, currentTime time.Time) (currentHealthy int32) {
	for _, pod := range pods {
		// 已进入删除流程的 Pod 不应再被视为健康。
		if pod.DeletionTimestamp != nil {
			continue
		}
		// 记录尚未过期的预期中断同样要提前扣除，防止同一 Pod 被重复允许驱逐。
		if disruptionTime, found := disruptedPods[pod.Name]; found && disruptionTime.Time.Add(DeletionTimeout).After(currentTime) {
			continue
		}
		if apipod.IsPodReady(pod) {
			currentHealthy++
		}
	}

	return
}

// buildDisruptedPodMap 重建 DisruptedPods 映射：丢弃已删除、不存在或始终未被删除的条目，
// 保留仍在等待删除的记录，并返回最近一次到期时间供调用方安排复查。
func (dc *DisruptionController) buildDisruptedPodMap(logger klog.Logger, pods []*v1.Pod, pdb *policy.PodDisruptionBudget, currentTime time.Time) (map[string]metav1.Time, *time.Time) {
	disruptedPods := pdb.Status.DisruptedPods
	result := make(map[string]metav1.Time)
	var recheckTime *time.Time

	if disruptedPods == nil {
		return result, recheckTime
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			// 已经在删除中，无需再追踪中断记录。
			continue
		}
		disruptionTime, found := disruptedPods[pod.Name]
		if !found {
			// 该 Pod 没有被记录为中断中，保持忽略。
			continue
		}
		expectedDeletion := disruptionTime.Time.Add(DeletionTimeout)
		if expectedDeletion.Before(currentTime) {
			logger.V(1).Info("pod was expected to be deleted but it wasn't, updating PDB",
				"pod", klog.KObj(pod), "deletionTime", disruptionTime, "podDisruptionBudget", klog.KObj(pdb))
			dc.recorder.Eventf(pod, v1.EventTypeWarning, "NotDeleted", "Pod was expected by PDB %s/%s to be deleted but it wasn't",
				pdb.Namespace, pdb.Namespace)
		} else {
			if recheckTime == nil || expectedDeletion.Before(*recheckTime) {
				recheckTime = &expectedDeletion
			}
			result[pod.Name] = disruptionTime
		}
	}
	return result, recheckTime
}

// failSafe 是最后的安全网：其他计算都失败时，至少把 DisruptionsAllowed 置为 0，
// 并设置 SyncFailed 条件。只要这次写回成功，/evict 处理器就不会在不安全时批准驱逐，
// 这是设计中"失败即保守"（fail open 的反面）的落点。
func (dc *DisruptionController) failSafe(ctx context.Context, pdb *policy.PodDisruptionBudget, err error) error {
	// 先深拷贝，避免失败路径修改到缓存中的原对象。
	newPdb := pdb.DeepCopy()
	newPdb.Status.DisruptionsAllowed = 0

	if newPdb.Status.Conditions == nil {
		newPdb.Status.Conditions = make([]metav1.Condition, 0)
	}
	apimeta.SetStatusCondition(&newPdb.Status.Conditions, metav1.Condition{
		Type:               policy.DisruptionAllowedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             policy.SyncFailedReason,
		Message:            err.Error(),
		ObservedGeneration: newPdb.Status.ObservedGeneration,
	})

	return dc.getUpdater()(ctx, newPdb)
}

// updatePdbStatus 计算新状态并仅在发生变化时写回。
// expectedCount <= 0 时强制把允许中断数置 0，防止新 Pod 刚出现、状态尚未刷新期间被误放行。
func (dc *DisruptionController) updatePdbStatus(ctx context.Context, pdb *policy.PodDisruptionBudget, currentHealthy, desiredHealthy, expectedCount int32,
	disruptedPods map[string]metav1.Time) error {

	// 要求 expectedCount > 0：PDB 当前没有匹配 Pod 时，若第一个 Pod 刚出现而状态还没更新，
	// 强制为 0 可以让这段窗口保持安全。这并非唯一的竞态，但却是常见且容易察觉的一种。
	disruptionsAllowed := currentHealthy - desiredHealthy
	if expectedCount <= 0 || disruptionsAllowed <= 0 {
		disruptionsAllowed = 0
	}

	// 所有字段都一致时直接返回，省掉无意义的 status 写入。
	if pdb.Status.CurrentHealthy == currentHealthy &&
		pdb.Status.DesiredHealthy == desiredHealthy &&
		pdb.Status.ExpectedPods == expectedCount &&
		pdb.Status.DisruptionsAllowed == disruptionsAllowed &&
		apiequality.Semantic.DeepEqual(pdb.Status.DisruptedPods, disruptedPods) &&
		pdb.Status.ObservedGeneration == pdb.Generation &&
		pdbhelper.ConditionsAreUpToDate(pdb) {
		return nil
	}

	newPdb := pdb.DeepCopy()
	newPdb.Status = policy.PodDisruptionBudgetStatus{
		CurrentHealthy:     currentHealthy,
		DesiredHealthy:     desiredHealthy,
		ExpectedPods:       expectedCount,
		DisruptionsAllowed: disruptionsAllowed,
		DisruptedPods:      disruptedPods,
		ObservedGeneration: pdb.Generation,
		Conditions:         newPdb.Status.Conditions,
	}

	pdbhelper.UpdateDisruptionAllowedCondition(newPdb)

	return dc.getUpdater()(ctx, newPdb)
}

// writePdbStatus 把新状态写回 API Server。这里不自行重试：
// 失败交给 processNextWorkItem 统一记录并按限速重试。
func (dc *DisruptionController) writePdbStatus(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	_, err := dc.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).UpdateStatus(ctx, pdb, metav1.UpdateOptions{})
	return err
}

// nonTerminatingPodHasStaleDisruptionCondition 判断非终止中 Pod 的 DisruptionTarget 条件是否陈旧，
// 并返回还需等待多久才能清理（0 表示可以立即清理）。
// kubelet 添加的条件和终态 Pod 上的条件一律不算陈旧，避免误清仍然有效的信号。
func (dc *DisruptionController) nonTerminatingPodHasStaleDisruptionCondition(pod *v1.Pod) (bool, time.Duration) {
	if pod.DeletionTimestamp != nil {
		return false, 0
	}
	_, cond := apipod.GetPodCondition(&pod.Status, v1.DisruptionTarget)
	// kubelet 写入的中断条件可能要很久之后 Pod 才真正进入终止流程（带 DeletionTimestamp），
	// 因此永远不视为陈旧；终态 Pod 上的条件同样不清理，以免产生无谓的 status 更新。
	if cond == nil || cond.Status != v1.ConditionTrue || cond.Reason == v1.PodReasonTerminationByKubelet || apipod.IsPodPhaseTerminal(pod.Status.Phase) {
		return false, 0
	}
	waitFor := dc.stalePodDisruptionTimeout - dc.clock.Since(cond.LastTransitionTime.Time)
	if waitFor < 0 {
		waitFor = 0
	}
	return true, waitFor
}
