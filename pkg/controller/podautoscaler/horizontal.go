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

package podautoscaler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	autoscalinginformers "k8s.io/client-go/informers/autoscaling/v2"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	autoscalingclient "k8s.io/client-go/kubernetes/typed/autoscaling/v2"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	autoscalinglisters "k8s.io/client-go/listers/autoscaling/v2"
	corelisters "k8s.io/client-go/listers/core/v1"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	metricsclient "k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
	"k8s.io/kubernetes/pkg/controller/podautoscaler/monitor"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/kubernetes/pkg/controller/util/selectors"
	"k8s.io/kubernetes/pkg/features"
)

// 本组变量保存 HPA 扩容计算中使用的默认参数与预定义错误。
var (
	// scaleUpLimitFactor 是默认的扩容倍率上限：单次最多扩到当前副本数的该倍数。
	scaleUpLimitFactor = 2.0
	// scaleUpLimitMinimum 是默认的扩容绝对上限：即使当前副本数很小，一次也至少允许扩到该值。
	scaleUpLimitMinimum = 4.0
)

// 本组变量定义用于标识错误来源的哨兵错误。
var (
	// errSpec 用于判断错误是否源自 reconcileAutoscaler 中 HPA 对象的 spec。
	// 所有这类错误都应以该错误为根错误，以便上层函数区分 spec 错误与内部错误。
	// 例如：fmt.Errorf("invalid spec%w", errSpec)
	errSpec error = errors.New("")
)

// 本组变量保存 HPA 资源标识等共享值。
var (
	// horizontalGroupResource 标识 HPA 资源，用于一致性存储的键。
	horizontalGroupResource = autoscalingv2.Resource("horizontalpodautoscalers")
)

// timestampedRecommendation 记录某次计算出的建议副本数及其产生时间，
// 用于在缩容稳定窗口内取历史建议值。
type timestampedRecommendation struct {
	// recommendation 是该时刻计算出的建议副本数。
	recommendation int32
	// timestamp 是建议的生成时间。
	timestamp time.Time
}

// timestampedScaleEvent 记录一次扩缩容事件，用于按策略周期限制扩缩容速率。
type timestampedScaleEvent struct {
	replicaChange int32 // 绝对值，非负
	// timestamp 是事件发生的时间。
	timestamp time.Time
	// outdated 标记该事件已超出最长策略周期，可被新事件复用。
	outdated bool
}

// HorizontalController 负责将系统中存储的 HPA 对象与其控制的实际
// Deployment/ReplicationController 保持同步。
type HorizontalController struct {
	// scaleNamespacer 用于读写目标的 scale 子资源。
	scaleNamespacer scaleclient.ScalesGetter
	// hpaNamespacer 用于更新 HPA 的 status 子资源。
	hpaNamespacer autoscalingclient.HorizontalPodAutoscalersGetter
	// mapper 用于把 scale target 的 GroupKind 映射到 RESTMapping。
	mapper apimeta.RESTMapper

	// tolerance 是未配置时的默认指标计算容差。
	tolerance float64
	// replicaCalc 根据指标计算建议副本数。
	replicaCalc *ReplicaCalculator
	// eventRecorder 用于记录 HPA 相关事件。
	eventRecorder record.EventRecorder

	// downscaleStabilisationWindow 是缩容建议的稳定窗口。
	downscaleStabilisationWindow time.Duration

	// monitor 用于上报控制器的指标。
	monitor monitor.Monitor

	// hpaLister 从 NewHorizontalController 传入的 informer 共享缓存中列出/获取 HPA。
	hpaLister autoscalinglisters.HorizontalPodAutoscalerLister
	// hpaListerSynced 指示 HPA informer 缓存是否已完成同步。
	hpaListerSynced cache.InformerSynced

	// podLister 从 NewHorizontalController 传入的 informer 共享缓存中列出/获取 Pod。
	podLister corelisters.PodLister
	// podListerSynced 指示 Pod informer 缓存是否已完成同步。
	podListerSynced cache.InformerSynced

	// queue 保存待同步的 HPA 键；使用限速队列避免热循环。
	queue workqueue.TypedRateLimitingInterface[string]

	// recommendations 保存每个 autoscaler 最近未稳定的建议副本数。
	recommendations map[string][]timestampedRecommendation
	// recommendationsLock 保护 recommendations。
	recommendationsLock sync.Mutex

	// scaleUpEvents 保存每个 HPA 最近的扩容事件，用于限制扩容速率。
	scaleUpEvents map[string][]timestampedScaleEvent
	// scaleUpEventsLock 保护 scaleUpEvents。
	scaleUpEventsLock sync.RWMutex
	// scaleDownEvents 保存每个 HPA 最近的缩容事件，用于限制缩容速率。
	scaleDownEvents map[string][]timestampedScaleEvent
	// scaleDownEventsLock 保护 scaleDownEvents。
	scaleDownEventsLock sync.RWMutex

	// selectorTracker 跟踪 HPA 及其 selector；具体实现由 HPAOptimizedSelectorStore 特性门控决定。
	selectorTracker hpaSelectorTracker

	// consistencyStore 用于跟踪控制器操作的状态，避免处理过期对象。
	consistencyStore consistencyutil.ConsistencyStore
}

// NewHorizontalController 创建一个新的 HorizontalController。
func NewHorizontalController(
	ctx context.Context,
	evtNamespacer v1core.EventsGetter,
	scaleNamespacer scaleclient.ScalesGetter,
	hpaNamespacer autoscalingclient.HorizontalPodAutoscalersGetter,
	mapper apimeta.RESTMapper,
	metricsClient metricsclient.MetricsClient,
	hpaInformer autoscalinginformers.HorizontalPodAutoscalerInformer,
	podInformer coreinformers.PodInformer,
	resyncPeriod time.Duration,
	downscaleStabilisationWindow time.Duration,
	tolerance float64,
	cpuInitializationPeriod,
	delayOfInitialReadinessStatus time.Duration,
) *HorizontalController {
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	broadcaster.StartStructuredLogging(3)
	broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: evtNamespacer.Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "horizontal-pod-autoscaler"})
	var consistencyStore consistencyutil.ConsistencyStore

	// 特性开启时用真实的一致性存储跟踪 HPA 状态，关闭时退化为 noop，
	// 避免为不需要该功能的用户引入额外开销。
	if utilfeature.DefaultFeatureGate.Enabled(features.StaleControllerConsistencyHPA) {
		consistencyStore = consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
			horizontalGroupResource: hpaInformer.Informer().GetStore(),
		})
	} else {
		consistencyStore = consistencyutil.NewNoopConsistencyStore()
	}

	hpaController := &HorizontalController{
		eventRecorder:                recorder,
		scaleNamespacer:              scaleNamespacer,
		hpaNamespacer:                hpaNamespacer,
		tolerance:                    tolerance,
		downscaleStabilisationWindow: downscaleStabilisationWindow,
		monitor:                      monitor.New(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			NewDefaultHPARateLimiter(resyncPeriod),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "horizontalpodautoscaler",
			},
		),
		mapper:              mapper,
		recommendations:     map[string][]timestampedRecommendation{},
		recommendationsLock: sync.Mutex{},
		scaleUpEvents:       map[string][]timestampedScaleEvent{},
		scaleUpEventsLock:   sync.RWMutex{},
		scaleDownEvents:     map[string][]timestampedScaleEvent{},
		scaleDownEventsLock: sync.RWMutex{},
		consistencyStore:    consistencyStore,
	}

	// 根据特性门控选择 selector 跟踪实现：优化版存储或双向多映射。
	if utilfeature.DefaultFeatureGate.Enabled(features.HPAOptimizedSelectorStore) {
		hpaController.selectorTracker = newHPASelectorStore()
	} else {
		hpaController.selectorTracker = newBiMultimapSelectorTracker()
	}

	// 注册 HPA 的增删改事件处理函数，并带上重同步周期，
	// 使 HPA 定期被重新入队以处理指标变化。
	hpaInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    hpaController.enqueueHPA,
			UpdateFunc: hpaController.updateHPA,
			DeleteFunc: hpaController.deleteHPA,
		},
		resyncPeriod,
	)
	hpaController.hpaLister = hpaInformer.Lister()
	hpaController.hpaListerSynced = hpaInformer.Informer().HasSynced

	hpaController.podLister = podInformer.Lister()
	hpaController.podListerSynced = podInformer.Informer().HasSynced

	replicaCalc := NewReplicaCalculator(
		metricsClient,
		hpaController.podLister,
		cpuInitializationPeriod,
		delayOfInitialReadinessStatus,
	)
	hpaController.replicaCalc = replicaCalc

	monitor.Register()

	return hpaController
}

// Run 开始监听并同步 HPA，直到 context 结束。
func (a *HorizontalController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting HPA controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down HPA controller")
		a.queue.ShutDown()
		wg.Wait()
	}()

	// 必须等缓存同步完成再启动 worker，否则会基于空缓存做出错误的扩缩容决策。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, a.hpaListerSynced, a.podListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, a.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// updateHPA 处理 HPA 更新事件。obj 可能为 *v1.HorizontalPodAutoscaler，
// 也可能是 DeletionFinalStateUnknown 标记对象。
func (a *HorizontalController) updateHPA(oldObj, curObj interface{}) {
	oldHPA, ok := oldObj.(*autoscalingv2.HorizontalPodAutoscaler)
	if !ok {
		a.enqueueRateLimited(curObj)
		return
	}
	curHPA, ok := curObj.(*autoscalingv2.HorizontalPodAutoscaler)
	if !ok {
		a.enqueueRateLimited(curObj)
		return
	}

	// HPAGeneration 开启时，API server 会在 spec 变更时递增 Generation，
	// 因此可以据此区分 spec 变更与仅 status 的更新：spec 变更立即调谐，
	// 仅 status 的更新（比如控制器自己写回的状态）继续走限速入队，
	// 避免形成热循环。见 https://issues.k8s.io/42715。
	if utilfeature.DefaultFeatureGate.Enabled(features.HPAGeneration) &&
		oldHPA.Generation != curHPA.Generation {
		a.enqueueHPA(curHPA)
		return
	}

	a.enqueueRateLimited(curHPA)
}

// enqueueRateLimited 以限速方式把对象入队，用于无需立即处理的更新。
func (a *HorizontalController) enqueueRateLimited(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %w", obj, err))
		return
	}
	a.queue.AddRateLimited(key)
}

// enqueueHPA 立即把 HPA 入队。obj 可能为 *v1.HorizontalPodAutoscaler，
// 也可能是 DeletionFinalStateUnknown 标记对象。
func (a *HorizontalController) enqueueHPA(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %w", obj, err))
		return
	}

	// 若 HPA 尚未注册到 selector 跟踪器，则先用一个不选中任何对象的 Nothing selector 占位。
	// 真正的选择器会在调谐时解析并更新。先注册可以避免立即调谐先于 selector 记录出现而引发竞态。
	hpaKey := selectors.Parse(key)
	if a.selectorTracker.PutIfAbsent(hpaKey.Namespace, hpaKey, labels.Nothing()) {
		a.monitor.ObserveHPAAddition()
	}

	// 立即入队；去重由队列负责：若键已在队列中或正在处理，则本次操作为空操作。
	a.queue.Add(key)
}

// deleteHPA 处理 HPA 删除事件：清理一致性状态、selector 记录和监控计数。
func (a *HorizontalController) deleteHPA(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %w", obj, err))
		return
	}

	hpa, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
	if !ok {
		if tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
			hpa, _ = tombstone.Obj.(*autoscalingv2.HorizontalPodAutoscaler)
		}
	}
	if hpa != nil {
		// 删除时清掉一致性状态，防止后续同名 HPA 被误判为旧对象而跳过调谐。
		a.consistencyStore.Clear(
			types.NamespacedName{Namespace: hpa.Namespace, Name: hpa.Name},
			hpa.UID,
		)
	}

	// TODO: could we leak if we fail to get the key?
	a.queue.Forget(key)

	// 删除 HPA 及其关联的 selector 记录。
	hpaKey := selectors.Parse(key)
	a.selectorTracker.Delete(hpaKey.Namespace, hpaKey)
	// 上报 HPA 删除指标。
	a.monitor.ObserveHPADeletion()
}

// worker 循环处理工作队列，直到队列关闭或 context 结束。
func (a *HorizontalController) worker(ctx context.Context) {
	for a.processNextWorkItem(ctx) {
	}
	logger := klog.FromContext(ctx)
	logger.Info("Horizontal Pod Autoscaler controller worker shutting down")
}

// processNextWorkItem 取出队列中的一个键并调谐；队列关闭时返回 false。
func (a *HorizontalController) processNextWorkItem(ctx context.Context) bool {
	key, quit := a.queue.Get()
	if quit {
		return false
	}
	defer a.queue.Done(key)

	deleted, err := a.reconcileKey(ctx, key)
	if err != nil {
		utilruntime.HandleError(err)
	}
	// 再次以 resyncPeriod 延迟把 HPA 入队，保证每隔一个 resyncPeriod 就会被处理一次。
	// 若队列中已有该 HPA 的请求，新请求会被丢弃。这里补一次入队是为了兜底：
	// resync 添加请求与旧请求出队之间存在竞态，若 resync 在旧请求出队前添加，
	// 新请求可能被丢弃，导致 HPA 延迟到 2 倍 resyncPeriod 才被处理。
	if !deleted {
		a.queue.AddRateLimited(key)
	}

	return true
}

// computeReplicasForMetrics 为 HPA 中列出的所有指标计算期望副本数，
// 返回各指标计算结果中的最大值、对应指标的描述，以及所有指标的状态。
// 当部分指标仍可用、HPA 应基于这些指标扩缩容时，可能同时返回有效的 metricDesiredReplicas 和错误。
// 若因错误导致完全无法计算，则在 metricDesiredReplicas 中返回 -1 作为失败信号。
func (a *HorizontalController) computeReplicasForMetrics(ctx context.Context, hpa *autoscalingv2.HorizontalPodAutoscaler, scale *autoscalingv1.Scale,
	metricSpecs []autoscalingv2.MetricSpec) (replicas int32, metric string, statuses []autoscalingv2.MetricStatus, timestamp time.Time, err error) {

	selector, err := a.validateAndParseSelector(hpa, scale.Status.Selector)
	if err != nil {
		return -1, "", nil, time.Time{}, err
	}

	specReplicas := scale.Spec.Replicas
	statusReplicas := scale.Status.Replicas
	statuses = make([]autoscalingv2.MetricStatus, len(metricSpecs))

	invalidMetricsCount := 0
	var invalidMetricError error
	var invalidMetricCondition autoscalingv2.HorizontalPodAutoscalerCondition

	for i, metricSpec := range metricSpecs {
		replicaCountProposal, metricNameProposal, timestampProposal, condition, err := a.computeReplicasForMetric(ctx, hpa, metricSpec, specReplicas, statusReplicas, selector, &statuses[i])

		// 单个指标失败不影响其他指标：记录下来继续处理，最后统一判断能否扩缩容。
		if err != nil {
			if invalidMetricsCount <= 0 {
				invalidMetricCondition = condition
				invalidMetricError = err
			}
			invalidMetricsCount++
			continue
		}
		// 多个指标时取最大的建议副本数，保证所有指标的需求都被满足。
		if replicas == 0 || replicaCountProposal > replicas {
			timestamp = timestampProposal
			replicas = replicaCountProposal
			metric = metricNameProposal
		}
	}

	if invalidMetricError != nil {
		invalidMetricError = fmt.Errorf("invalid metrics (%v invalid out of %v), first error is: %w", invalidMetricsCount, len(metricSpecs), invalidMetricError)
	}

	// 若所有指标都无效，或部分指标无效且我们将要缩容，则不能安全地基于不完整数据缩容：
	// 返回错误并按第一个无效指标设置条件。否则说明仍可安全扩容，标记为 ScalingActive。
	if invalidMetricsCount >= len(metricSpecs) || (invalidMetricsCount > 0 && replicas < specReplicas) {
		setCondition(hpa, invalidMetricCondition.Type, invalidMetricCondition.Status, invalidMetricCondition.Reason, "%s", invalidMetricCondition.Message)
		return -1, "", statuses, time.Time{}, invalidMetricError
	}
	setCondition(hpa, autoscalingv2.ScalingActive, v1.ConditionTrue, "ValidMetricFound", "the HPA was able to successfully calculate a replica count from %s", metric)

	return replicas, metric, statuses, timestamp, invalidMetricError
}

// validateAndParseSelector 校验并解析 scale target 的 selector：
// - selector 不能为空；
// - selector 格式必须合法；
// - 当前 selector 选中的所有 Pod 只能被一个 HPA 控制。
// 校验失败时返回错误，成功时返回解析后的 selector。
// 出错时会以相应 reason 将 ScalingActive 条件设为 false。
func (a *HorizontalController) validateAndParseSelector(hpa *autoscalingv2.HorizontalPodAutoscaler, selector string) (labels.Selector, error) {
	if selector == "" {
		errMsg := "selector is required"
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "SelectorRequired", errMsg)
		setCondition(hpa, autoscalingv2.ScalingActive, v1.ConditionFalse, "InvalidSelector", "the HPA target's scale is missing a selector")
		return nil, errors.New(errMsg)
	}

	parsedSelector, err := labels.Parse(selector)
	if err != nil {
		errMsg := fmt.Sprintf("couldn't convert selector into a corresponding internal selector object: %v", err)
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "InvalidSelector", errMsg)
		setCondition(hpa, autoscalingv2.ScalingActive, v1.ConditionFalse, "InvalidSelector", "%s", errMsg)
		return nil, errors.New(errMsg)
	}

	hpaKey := selectors.Key{Name: hpa.Name, Namespace: hpa.Namespace}
	// 只有当 HPA 已在 enqueueHPA 中注册时才更新其 selector。
	if !a.selectorTracker.PutIfPresent(hpaKey.Namespace, hpaKey, parsedSelector) {
		// 说明 HPA 在调谐期间已被删除，直接短路返回。
		return nil, fmt.Errorf("HPA %s/%s was deleted during reconciliation", hpa.Namespace, hpa.Name)
	}

	pods, err := a.podLister.Pods(hpa.Namespace).List(parsedSelector)
	if err != nil {
		return nil, err
	}

	selectingHpas := a.selectorTracker.HPAsMatchingPods(hpa.Namespace, pods, 2)
	if len(selectingHpas) > 1 {
		errMsg := fmt.Sprintf("pods by selector %v are controlled by more than one HPA (e.g. %v)", selector, selectingHpas)
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "AmbiguousSelector", errMsg)
		setCondition(hpa, autoscalingv2.ScalingActive, v1.ConditionFalse, "AmbiguousSelector", "%s", errMsg)
		return nil, errors.New(errMsg)
	}

	return parsedSelector, nil
}

// computeReplicasForMetric 为指定 HPA 和单条指标规格计算期望副本数，
// 返回指标状态以及建议设置到 HPA 对象上的条件。
func (a *HorizontalController) computeReplicasForMetric(ctx context.Context, hpa *autoscalingv2.HorizontalPodAutoscaler, spec autoscalingv2.MetricSpec,
	specReplicas, statusReplicas int32, selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, metricNameProposal string,
	timestampProposal time.Time, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	// actionLabel 用于上报本次调谐采取了哪种动作。
	start := time.Now()
	defer func() {
		actionLabel := monitor.ActionLabelNone
		switch {
		case replicaCountProposal > hpa.Status.CurrentReplicas:
			actionLabel = monitor.ActionLabelScaleUp
		case replicaCountProposal < hpa.Status.CurrentReplicas:
			actionLabel = monitor.ActionLabelScaleDown
		}

		errorLabel := monitor.ErrorLabelNone
		if err != nil {
			// 出错时默认归类为内部错误，且不计入扩缩容动作，避免误报。
			errorLabel = monitor.ErrorLabelInternal
			actionLabel = monitor.ActionLabelNone
		}
		if errors.Is(err, errSpec) {
			// 能区分出 spec 错误时单独归类，便于定位用户配置问题。
			errorLabel = monitor.ErrorLabelSpec
		}

		a.monitor.ObserveMetricComputationResult(actionLabel, errorLabel, time.Since(start), spec.Type)
	}()

	switch spec.Type {
	case autoscalingv2.ObjectMetricSourceType:
		metricSelector, err := metav1.LabelSelectorAsSelector(spec.Object.Metric.Selector)
		if err != nil {
			condition := a.getUnableComputeReplicaCountCondition(hpa, "FailedGetObjectMetric", err)
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get object metric value: %w", err)
		}
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = a.computeStatusForObjectMetric(specReplicas, statusReplicas, spec, hpa, selector, status, metricSelector)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get object metric value: %w", err)
		}
	case autoscalingv2.PodsMetricSourceType:
		metricSelector, err := metav1.LabelSelectorAsSelector(spec.Pods.Metric.Selector)
		if err != nil {
			condition := a.getUnableComputeReplicaCountCondition(hpa, "FailedGetPodsMetric", err)
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get pods metric value: %w", err)
		}
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = a.computeStatusForPodsMetric(specReplicas, spec, hpa, selector, status, metricSelector)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get pods metric value: %w", err)
		}
	case autoscalingv2.ResourceMetricSourceType:
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = a.computeStatusForResourceMetric(ctx, specReplicas, spec, hpa, selector, status)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get %s resource metric value: %w", spec.Resource.Name, err)
		}
	case autoscalingv2.ContainerResourceMetricSourceType:
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = a.computeStatusForContainerResourceMetric(ctx, specReplicas, spec, hpa, selector, status)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get %s container metric value: %w", spec.ContainerResource.Container, err)
		}
	case autoscalingv2.ExternalMetricSourceType:
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = a.computeStatusForExternalMetric(specReplicas, statusReplicas, spec, hpa, selector, status)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get %s external metric value: %w", spec.External.Metric.Name, err)
		}
	default:
		// 正常情况下不会走到这里：api-server 的校验会过滤掉非法的指标源类型。
		// 这里用 errSpec 包装，便于上层把它识别为 spec 错误。
		err = fmt.Errorf("unknown metric source type %q%w", string(spec.Type), errSpec)
		condition := a.getUnableComputeReplicaCountCondition(hpa, "InvalidMetricSourceType", err)
		return 0, "", time.Time{}, condition, err
	}
	return replicaCountProposal, metricNameProposal, timestampProposal, autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
}

// reconcileKey 根据队列中的键调谐单个 HPA；返回 deleted 表示 HPA 已被删除，无需再重新入队。
func (a *HorizontalController) reconcileKey(ctx context.Context, key string) (deleted bool, err error) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return true, err
	}

	logger := klog.FromContext(ctx)

	hpaNamespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	if err := a.consistencyStore.EnsureReady(hpaNamespacedName); err != nil {
		if consistencyErr, ok := errors.AsType[*consistencyutil.ConsistencyError](err); ok {
			a.monitor.ObserveHPARequeueSkips(
				consistencyErr.GroupResource.Group,
				consistencyErr.GroupResource.Resource,
			)
		}
		return false, err
	}

	hpa, err := a.hpaLister.HorizontalPodAutoscalers(namespace).Get(name)
	if k8serrors.IsNotFound(err) {
		// HPA 已删除：清理所有按 key 索引的临时状态，避免内存泄漏并防止同名新 HPA 复用旧数据。
		a.consistencyStore.Clear(hpaNamespacedName, "")
		logger.Info("Horizontal Pod Autoscaler has been deleted", "HPA", klog.KRef(namespace, name))

		a.recommendationsLock.Lock()
		delete(a.recommendations, key)
		a.recommendationsLock.Unlock()

		a.scaleUpEventsLock.Lock()
		delete(a.scaleUpEvents, key)
		a.scaleUpEventsLock.Unlock()

		a.scaleDownEventsLock.Lock()
		delete(a.scaleDownEvents, key)
		a.scaleDownEventsLock.Unlock()

		return true, nil
	}
	if err != nil {
		return false, err
	}

	return false, a.reconcileAutoscaler(ctx, hpa, key)
}

// computeStatusForObjectMetric 为 ObjectMetricSourceType 类型的指定指标计算期望副本数。
func (a *HorizontalController) computeStatusForObjectMetric(specReplicas, statusReplicas int32, metricSpec autoscalingv2.MetricSpec, hpa *autoscalingv2.HorizontalPodAutoscaler, selector labels.Selector, status *autoscalingv2.MetricStatus, metricSelector labels.Selector) (replicas int32, timestamp time.Time, metricName string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	metricStatus := autoscalingv2.MetricStatus{
		Type: autoscalingv2.ObjectMetricSourceType,
		Object: &autoscalingv2.ObjectMetricStatus{
			DescribedObject: metricSpec.Object.DescribedObject,
			Metric: autoscalingv2.MetricIdentifier{
				Name:     metricSpec.Object.Metric.Name,
				Selector: metricSpec.Object.Metric.Selector,
			},
		},
	}
	tolerances := a.tolerancesForHpa(hpa)
	if metricSpec.Object.Target.Type == autoscalingv2.ValueMetricType && metricSpec.Object.Target.Value != nil {
		replicaCountProposal, usageProposal, timestampProposal, err := a.replicaCalc.GetObjectMetricReplicas(specReplicas, metricSpec.Object.Target.Value.MilliValue(), metricSpec.Object.Metric.Name, tolerances, hpa.Namespace, &metricSpec.Object.DescribedObject, selector, metricSelector)
		if err != nil {
			condition := a.getUnableComputeReplicaCountCondition(hpa, "FailedGetObjectMetric", err)
			return 0, timestampProposal, "", condition, err
		}
		metricStatus.Object.Current.Value = resource.NewMilliQuantity(usageProposal, resource.DecimalSI)
		*status = metricStatus
		return replicaCountProposal, timestampProposal, fmt.Sprintf("%s metric %s", metricSpec.Object.DescribedObject.Kind, metricSpec.Object.Metric.Name), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	} else if metricSpec.Object.Target.Type == autoscalingv2.AverageValueMetricType && metricSpec.Object.Target.AverageValue != nil {
		// 平均值目标按 Pod 维度计算，因此传入 statusReplicas 而非 specReplicas。
		replicaCountProposal, usageProposal, timestampProposal, err := a.replicaCalc.GetObjectPerPodMetricReplicas(statusReplicas, metricSpec.Object.Target.AverageValue.MilliValue(), metricSpec.Object.Metric.Name, tolerances, hpa.Namespace, &metricSpec.Object.DescribedObject, metricSelector)
		if err != nil {
			condition := a.getUnableComputeReplicaCountCondition(hpa, "FailedGetObjectMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get %s object metric: %w", metricSpec.Object.Metric.Name, err)
		}
		metricStatus.Object.Current.AverageValue = resource.NewMilliQuantity(usageProposal, resource.DecimalSI)
		*status = metricStatus
		return replicaCountProposal, timestampProposal, fmt.Sprintf("external metric %s(%+v)", metricSpec.Object.Metric.Name, metricSpec.Object.Metric.Selector), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	errMsg := "invalid object metric source: neither a value target nor an average value target was set"
	err = errors.New(errMsg)
	condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetObjectMetric", err)
	return 0, time.Time{}, "", condition, err
}

// computeStatusForPodsMetric 为 PodsMetricSourceType 类型的指定指标计算期望副本数。
func (a *HorizontalController) computeStatusForPodsMetric(currentReplicas int32, metricSpec autoscalingv2.MetricSpec, hpa *autoscalingv2.HorizontalPodAutoscaler, selector labels.Selector, status *autoscalingv2.MetricStatus, metricSelector labels.Selector) (replicaCountProposal int32, timestampProposal time.Time, metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	tolerances := a.tolerancesForHpa(hpa)
	replicaCountProposal, usageProposal, timestampProposal, err := a.replicaCalc.GetMetricReplicas(currentReplicas, metricSpec.Pods.Target.AverageValue.MilliValue(), metricSpec.Pods.Metric.Name, tolerances, hpa.Namespace, selector, metricSelector)
	if err != nil {
		condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetPodsMetric", err)
		return 0, timestampProposal, "", condition, err
	}
	*status = autoscalingv2.MetricStatus{
		Type: autoscalingv2.PodsMetricSourceType,
		Pods: &autoscalingv2.PodsMetricStatus{
			Metric: autoscalingv2.MetricIdentifier{
				Name:     metricSpec.Pods.Metric.Name,
				Selector: metricSpec.Pods.Metric.Selector,
			},
			Current: autoscalingv2.MetricValueStatus{
				AverageValue: resource.NewMilliQuantity(usageProposal, resource.DecimalSI),
			},
		},
	}

	return replicaCountProposal, timestampProposal, fmt.Sprintf("pods metric %s", metricSpec.Pods.Metric.Name), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
}

// computeStatusForResourceMetricGeneric 是资源类指标（含容器资源指标）计算的公共实现，
// 支持平均值（AverageValue）和平均利用率（AverageUtilization）两种目标。
func (a *HorizontalController) computeStatusForResourceMetricGeneric(ctx context.Context, currentReplicas int32, target autoscalingv2.MetricTarget,
	resourceName v1.ResourceName, hpa *autoscalingv2.HorizontalPodAutoscaler, container string, selector labels.Selector, sourceType autoscalingv2.MetricSourceType) (replicaCountProposal int32,
	metricStatus *autoscalingv2.MetricValueStatus, timestampProposal time.Time, metricNameProposal string,
	condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	namespace := hpa.Namespace
	tolerances := a.tolerancesForHpa(hpa)
	if target.AverageValue != nil {
		var rawProposal int64
		replicaCountProposal, rawProposal, timestampProposal, err := a.replicaCalc.GetRawResourceReplicas(ctx, currentReplicas, target.AverageValue.MilliValue(), resourceName, tolerances, namespace, selector, container)
		if err != nil {
			return 0, nil, time.Time{}, "", condition, fmt.Errorf("failed to get %s usage: %w", resourceName, err)
		}
		metricNameProposal = fmt.Sprintf("%s resource", resourceName.String())
		quantity := buildQuantity(resourceName, rawProposal)
		status := autoscalingv2.MetricValueStatus{
			AverageValue: &quantity,
		}
		return replicaCountProposal, &status, timestampProposal, metricNameProposal, autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}

	if target.AverageUtilization == nil {
		errMsg := "invalid resource metric source: neither an average utilization target nor an average value (usage) target was set"
		return 0, nil, time.Time{}, "", condition, errors.New(errMsg)
	}

	targetUtilization := *target.AverageUtilization
	replicaCountProposal, percentageProposal, rawProposal, timestampProposal, err := a.replicaCalc.GetResourceReplicas(ctx, currentReplicas, targetUtilization, resourceName, tolerances, namespace, selector, container)
	if err != nil {
		return 0, nil, time.Time{}, "", condition, fmt.Errorf("failed to get %s utilization: %w", resourceName, err)
	}

	metricNameProposal = fmt.Sprintf("%s resource utilization (percentage of request)", resourceName)
	if sourceType == autoscalingv2.ContainerResourceMetricSourceType {
		metricNameProposal = fmt.Sprintf("%s container resource utilization (percentage of request)", resourceName)
	}
	quantity := buildQuantity(resourceName, rawProposal)
	status := autoscalingv2.MetricValueStatus{
		AverageUtilization: &percentageProposal,
		AverageValue:       &quantity,
	}
	return replicaCountProposal, &status, timestampProposal, metricNameProposal, autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
}

// buildQuantity 根据资源类型为 HPA 指标构造 resource.Quantity。
//
// 对内存，rawProposal 以字节为单位，使用 BinarySI 格式（Ki/Mi/Gi）；
// 对 CPU 或其他资源，rawProposal 以毫为单位（例如 500 表示 0.5 核），使用 DecimalSI 格式（m）。
func buildQuantity(resourceName v1.ResourceName, rawProposal int64) resource.Quantity {
	format := resource.DecimalSI
	// 内存使用 BinarySI 以与 metrics-server 返回的格式保持一致，
	// 参见 https://github.com/kubernetes-sigs/metrics-server/blob/55b4961bc1eceffd0a37809dc271e9ae38de9deb/pkg/storage/types.go#L63-L64
	if resourceName == v1.ResourceMemory {
		format = resource.BinarySI
	}
	return *resource.NewMilliQuantity(rawProposal, format)
}

// computeStatusForResourceMetric 为 ResourceMetricSourceType 类型的指定指标计算期望副本数。
func (a *HorizontalController) computeStatusForResourceMetric(ctx context.Context, currentReplicas int32, metricSpec autoscalingv2.MetricSpec, hpa *autoscalingv2.HorizontalPodAutoscaler,
	selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, timestampProposal time.Time,
	metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	replicaCountProposal, metricValueStatus, timestampProposal, metricNameProposal, condition, err := a.computeStatusForResourceMetricGeneric(ctx, currentReplicas, metricSpec.Resource.Target, metricSpec.Resource.Name, hpa, "", selector, autoscalingv2.ResourceMetricSourceType)
	if err != nil {
		condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetResourceMetric", err)
		return replicaCountProposal, timestampProposal, metricNameProposal, condition, err
	}
	*status = autoscalingv2.MetricStatus{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricStatus{
			Name:    metricSpec.Resource.Name,
			Current: *metricValueStatus,
		},
	}
	return replicaCountProposal, timestampProposal, metricNameProposal, condition, nil
}

// computeStatusForContainerResourceMetric 为 ContainerResourceMetricSourceType 类型的指定指标计算期望副本数。
func (a *HorizontalController) computeStatusForContainerResourceMetric(ctx context.Context, currentReplicas int32, metricSpec autoscalingv2.MetricSpec, hpa *autoscalingv2.HorizontalPodAutoscaler,
	selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, timestampProposal time.Time,
	metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	replicaCountProposal, metricValueStatus, timestampProposal, metricNameProposal, condition, err := a.computeStatusForResourceMetricGeneric(ctx, currentReplicas, metricSpec.ContainerResource.Target, metricSpec.ContainerResource.Name, hpa, metricSpec.ContainerResource.Container, selector, autoscalingv2.ContainerResourceMetricSourceType)
	if err != nil {
		condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetContainerResourceMetric", err)
		return replicaCountProposal, timestampProposal, metricNameProposal, condition, err
	}
	*status = autoscalingv2.MetricStatus{
		Type: autoscalingv2.ContainerResourceMetricSourceType,
		ContainerResource: &autoscalingv2.ContainerResourceMetricStatus{
			Name:      metricSpec.ContainerResource.Name,
			Container: metricSpec.ContainerResource.Container,
			Current:   *metricValueStatus,
		},
	}
	return replicaCountProposal, timestampProposal, metricNameProposal, condition, nil
}

// computeStatusForExternalMetric 为 ExternalMetricSourceType 类型的指定指标计算期望副本数。
func (a *HorizontalController) computeStatusForExternalMetric(specReplicas, statusReplicas int32, metricSpec autoscalingv2.MetricSpec, hpa *autoscalingv2.HorizontalPodAutoscaler, selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, timestampProposal time.Time, metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	tolerances := a.tolerancesForHpa(hpa)
	if metricSpec.External.Target.AverageValue != nil {
		replicaCountProposal, usageProposal, timestampProposal, err := a.replicaCalc.GetExternalPerPodMetricReplicas(statusReplicas, metricSpec.External.Target.AverageValue.MilliValue(), metricSpec.External.Metric.Name, tolerances, hpa.Namespace, metricSpec.External.Metric.Selector)
		if err != nil {
			condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetExternalMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get %s external metric: %w", metricSpec.External.Metric.Name, err)
		}
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ExternalMetricSourceType,
			External: &autoscalingv2.ExternalMetricStatus{
				Metric: autoscalingv2.MetricIdentifier{
					Name:     metricSpec.External.Metric.Name,
					Selector: metricSpec.External.Metric.Selector,
				},
				Current: autoscalingv2.MetricValueStatus{
					AverageValue: resource.NewMilliQuantity(usageProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, fmt.Sprintf("external metric %s(%+v)", metricSpec.External.Metric.Name, metricSpec.External.Metric.Selector), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	if metricSpec.External.Target.Value != nil {
		replicaCountProposal, usageProposal, timestampProposal, err := a.replicaCalc.GetExternalMetricReplicas(specReplicas, metricSpec.External.Target.Value.MilliValue(), metricSpec.External.Metric.Name, tolerances, hpa.Namespace, metricSpec.External.Metric.Selector, selector)
		if err != nil {
			condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetExternalMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get external metric %s: %w", metricSpec.External.Metric.Name, err)
		}
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ExternalMetricSourceType,
			External: &autoscalingv2.ExternalMetricStatus{
				Metric: autoscalingv2.MetricIdentifier{
					Name:     metricSpec.External.Metric.Name,
					Selector: metricSpec.External.Metric.Selector,
				},
				Current: autoscalingv2.MetricValueStatus{
					Value: resource.NewMilliQuantity(usageProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, fmt.Sprintf("external metric %s(%+v)", metricSpec.External.Metric.Name, metricSpec.External.Metric.Selector), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	errMsg := "invalid external metric source: neither a value target nor an average value target was set"
	err = errors.New(errMsg)
	condition = a.getUnableComputeReplicaCountCondition(hpa, "FailedGetExternalMetric", err)
	return 0, time.Time{}, "", condition, errors.New(errMsg)
}

// recordInitialRecommendation 为 HPA 记录一条初始建议，供后续稳定窗口计算使用。
func (a *HorizontalController) recordInitialRecommendation(currentReplicas int32, key string) {
	a.recommendationsLock.Lock()
	defer a.recommendationsLock.Unlock()
	// 只在首次记录时写入，避免覆盖调谐过程中已经积累的历史建议。
	if a.recommendations[key] == nil {
		a.recommendations[key] = []timestampedRecommendation{{currentReplicas, time.Now()}}
	}
}

// shouldComputeMetricsForZeroReplicas 决定当前副本数为零时的扩缩容行为。
// 返回：
//   - needsMetricComputation：为 true 时需要计算指标以确定期望副本数；
//   - shouldDisable：为 true 时应禁用扩缩容（工作负载被手动缩容到了零）。
func (a *HorizontalController) shouldComputeMetricsForZeroReplicas(
	minReplicas int32,
	scaledToZeroCondition, canScaleFromZero bool,
) (needsMetricComputation bool, shouldDisable bool) {
	if (minReplicas != 0 && scaledToZeroCondition) || canScaleFromZero {
		return true, false
	}
	return false, true
}

// reconcileAutoscaler 调谐单个 HPA：获取目标 scale、计算指标、决定是否扩缩容并更新状态。
func (a *HorizontalController) reconcileAutoscaler(ctx context.Context, hpaShared *autoscalingv2.HorizontalPodAutoscaler, key string) (retErr error) {
	// actionLabel 用于上报本次调谐采取了哪种动作。
	actionLabel := monitor.ActionLabelNone
	start := time.Now()
	defer func() {
		errorLabel := monitor.ErrorLabelNone
		if retErr != nil {
			// 出错时默认归类为内部错误。
			errorLabel = monitor.ErrorLabelInternal
		}
		if errors.Is(retErr, errSpec) {
			// 能区分出 spec 错误时单独归类，便于定位用户配置问题。
			errorLabel = monitor.ErrorLabelSpec
		}

		a.monitor.ObserveReconciliationResult(actionLabel, errorLabel, time.Since(start))
	}()

	// 必须深拷贝：后续会修改 HPA（转换过程也可能修改对象），
	// 绝不能直接改动 informer 共享缓存里的对象。
	hpa := hpaShared.DeepCopy()
	hpaStatusOriginal := hpa.Status.DeepCopy()

	reference := fmt.Sprintf("%s/%s/%s", hpa.Spec.ScaleTargetRef.Kind, hpa.Namespace, hpa.Spec.ScaleTargetRef.Name)

	targetGV, err := schema.ParseGroupVersion(hpa.Spec.ScaleTargetRef.APIVersion)
	if err != nil {
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "FailedGetScale", err.Error())
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedGetScale", "the HPA controller was unable to get the target's current scale: %v", err)
		if err := a.updateStatusIfNeeded(ctx, hpaStatusOriginal, hpa); err != nil {
			utilruntime.HandleError(err)
		}
		return fmt.Errorf("invalid API version in scale target reference: %v%w", err, errSpec)
	}

	targetGK := schema.GroupKind{
		Group: targetGV.Group,
		Kind:  hpa.Spec.ScaleTargetRef.Kind,
	}

	mappings, err := a.mapper.RESTMappings(targetGK)
	if err != nil {
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "FailedGetScale", err.Error())
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedGetScale", "the HPA controller was unable to get the target's current scale: %v", err)
		if err := a.updateStatusIfNeeded(ctx, hpaStatusOriginal, hpa); err != nil {
			utilruntime.HandleError(err)
		}
		return fmt.Errorf("unable to determine resource for scale target reference: %w", err)
	}

	scale, targetGR, err := a.scaleForResourceMappings(ctx, hpa.Namespace, hpa.Spec.ScaleTargetRef.Name, mappings)
	if err != nil {
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "FailedGetScale", err.Error())
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedGetScale", "the HPA controller was unable to get the target's current scale: %v", err)
		if err := a.updateStatusIfNeeded(ctx, hpaStatusOriginal, hpa); err != nil {
			utilruntime.HandleError(err)
		}
		return fmt.Errorf("failed to query scale subresource for %s: %w", reference, err)
	}
	setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionTrue, "SucceededGetScale", "the HPA controller was able to get the target's current scale")
	currentReplicas := scale.Spec.Replicas
	a.recordInitialRecommendation(currentReplicas, key)

	var (
		metricStatuses        []autoscalingv2.MetricStatus
		metricDesiredReplicas int32
		metricName            string
	)

	desiredReplicas := int32(0)
	rescaleReason := ""

	var minReplicas int32

	if hpa.Spec.MinReplicas != nil {
		minReplicas = *hpa.Spec.MinReplicas
	} else {
		// MinReplicas 未设置时默认为 1。
		minReplicas = 1
	}

	rescale := true
	logger := klog.FromContext(ctx)

	// 预先计算与缩容到零相关的条件，避免在分支中重复判断。
	scaleToZeroFeatureEnabled := utilfeature.DefaultFeatureGate.Enabled(features.HPAScaleToZero)
	hasObjectOrExtMetrics := hasObjectOrExternalMetrics(hpa)
	scaledToZeroCondition := scaleToZeroFeatureEnabled && getScaledToZeroConditionStatus(hpa)
	canScaleFromZero := scaledToZeroCondition && hasObjectOrExtMetrics

	needsMetricComputation := true

	if currentReplicas == 0 {
		var shouldDisable bool
		needsMetricComputation, shouldDisable = a.shouldComputeMetricsForZeroReplicas(minReplicas, scaledToZeroCondition, canScaleFromZero)
		if shouldDisable {
			// 没有对象/外部指标可供从零扩容，或工作负载是被手动缩到零的，保持零副本并禁用扩缩容。
			desiredReplicas = 0
			rescale = false
			setCondition(hpa, autoscalingv2.ScalingActive, v1.ConditionFalse, "ScalingDisabled", "scaling is disabled since the replica count of the target is zero")
			if !scaleToZeroFeatureEnabled {
				removeCondition(hpa, autoscalingv2.ScaledToZero)
			}
		}
	} else if currentReplicas > hpa.Spec.MaxReplicas {
		// 当前副本数越界时无需计算指标，直接钳制到边界值。
		rescaleReason = "Current number of replicas above Spec.MaxReplicas"
		desiredReplicas = hpa.Spec.MaxReplicas
		needsMetricComputation = false
	} else if currentReplicas < minReplicas {
		rescaleReason = "Current number of replicas below Spec.MinReplicas"
		desiredReplicas = minReplicas
		needsMetricComputation = false
	}

	// 仅在需要基于指标扩缩容时才计算指标并归一化期望副本数
	if needsMetricComputation {
		var metricTimestamp time.Time
		metricDesiredReplicas, metricName, metricStatuses, metricTimestamp, err = a.computeReplicasForMetrics(ctx, hpa, scale, hpa.Spec.Metrics)
		// computeReplicasForMetrics 可能同时返回有效的副本数和错误，
		// 表示部分指标仍可用，HPA 应基于这些指标继续扩缩容。
		if err != nil && metricDesiredReplicas == -1 {
			a.setCurrentReplicasAndMetricsInStatus(hpa, currentReplicas, metricStatuses)
			if err := a.updateStatusIfNeeded(ctx, hpaStatusOriginal, hpa); err != nil {
				utilruntime.HandleError(err)
			}
			a.eventRecorder.Event(hpa, v1.EventTypeWarning, "FailedComputeMetricsReplicas", err.Error())
			return fmt.Errorf("failed to compute desired number of replicas based on listed metrics for %s: %w", reference, err)
		}
		if err != nil {
			// 部分指标无效但其余可用，继续执行扩缩容，但最后仍把错误返回给调用方上报。
			retErr = err
		}

		logMessage := "Proposing desired replicas"
		if currentReplicas == 0 {
			logMessage = "Proposing desired replicas from zero"
		}
		logger.V(4).Info(logMessage,
			"desiredReplicas", metricDesiredReplicas,
			"metric", metricName,
			"tolerances", a.tolerancesForHpa(hpa),
			"timestamp", metricTimestamp,
			"scaleTarget", reference)

		rescaleMetric := ""
		if metricDesiredReplicas > desiredReplicas {
			desiredReplicas = metricDesiredReplicas
			rescaleMetric = metricName
		}
		if desiredReplicas > currentReplicas {
			rescaleReason = fmt.Sprintf("%s above target", rescaleMetric)
		}
		if desiredReplicas < currentReplicas {
			rescaleReason = "All metrics below target"
		}
		if hpa.Spec.Behavior == nil {
			desiredReplicas = a.normalizeDesiredReplicas(hpa, key, currentReplicas, desiredReplicas, minReplicas)
		} else {
			desiredReplicas = a.normalizeDesiredReplicasWithBehaviors(hpa, key, currentReplicas, desiredReplicas, minReplicas)
		}
		// 从零扩容且 minReplicas 被调大时，确保至少扩到 minReplicas，
		// 否则指标计算结果可能低于新的下限。
		if currentReplicas == 0 && minReplicas != 0 && scaledToZeroCondition && desiredReplicas < minReplicas {
			desiredReplicas = minReplicas
			if rescaleReason == "" {
				rescaleReason = "Current number of replicas below Spec.MinReplicas"
			}
		}
		rescale = desiredReplicas != currentReplicas
	}
	if rescale {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			// 每次重试都设置期望副本数。这里无需深拷贝：scale 是直接从 API server
			// 读取的，不是共享缓存中的对象，可安全修改。
			scale.Spec.Replicas = desiredReplicas

			// 尝试更新 scale 子资源。
			_, updateErr := a.scaleNamespacer.Scales(hpa.Namespace).Update(ctx, targetGR, scale, metav1.UpdateOptions{})

			if updateErr == nil {
				return nil // 更新成功
			}

			// 更新失败说明可能发生了冲突，重新获取最新版本以刷新 resourceVersion，供下一次重试使用。
			latestScale, getErr := a.scaleNamespacer.Scales(hpa.Namespace).Get(ctx, targetGR, hpa.Spec.ScaleTargetRef.Name, metav1.GetOptions{})
			if getErr == nil {
				// 用最新版本替换本地 scale 对象。
				scale = latestScale
			} else {
				utilruntime.HandleError(fmt.Errorf("error getting latest scale for %s during conflict retry: %w", reference, getErr))
			}

			// 返回原始的更新错误，由 RetryOnConflict 判断是否需要重试。
			return updateErr
		})

		if err != nil {
			// 重试耗尽或出现非冲突错误时走到这里。
			a.eventRecorder.Eventf(hpa, v1.EventTypeWarning, "FailedRescale", "New size: %d; reason: %s; error: %v", desiredReplicas, rescaleReason, err.Error())
			setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedUpdateScale", "the HPA controller was unable to update the target scale: %v", err)
			a.setCurrentReplicasAndMetricsInStatus(hpa, currentReplicas, metricStatuses)
			if err := a.updateStatusIfNeeded(ctx, hpaStatusOriginal, hpa); err != nil {
				utilruntime.HandleError(err)
			}
			return fmt.Errorf("failed to rescale %s: %w", reference, err)
		}

		// 只有扩缩容成功才会走到这里。
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionTrue, "SucceededRescale", "the HPA controller was able to update the target scale to %d", desiredReplicas)
		a.eventRecorder.Eventf(hpa, v1.EventTypeNormal, "SuccessfulRescale", "New size: %d; reason: %s", desiredReplicas, rescaleReason)
		a.storeScaleEvent(hpa.Spec.Behavior, key, currentReplicas, desiredReplicas)
		logger.Info("Successfully rescaled",
			"HPA", klog.KObj(hpa),
			"currentReplicas", currentReplicas,
			"desiredReplicas", desiredReplicas,
			"reason", rescaleReason)

		// 每次扩缩容都更新 ScaledToZero 条件，避免该条件因状态残留而过时。
		if scaleToZeroFeatureEnabled {
			if currentReplicas > 0 && desiredReplicas == 0 && minReplicas == 0 && hasObjectOrExtMetrics {
				setCondition(hpa, autoscalingv2.ScaledToZero, v1.ConditionTrue, "ScaledToZero", "the HPA controller scaled the workload to zero")
			} else {
				setCondition(hpa, autoscalingv2.ScaledToZero, v1.ConditionFalse, "NotScaledToZero", "the HPA controller did not scale the workload to zero")
			}
		} else {
			removeCondition(hpa, autoscalingv2.ScaledToZero)
		}

		if desiredReplicas > currentReplicas {
			actionLabel = monitor.ActionLabelScaleUp
		} else {
			actionLabel = monitor.ActionLabelScaleDown
		}
	} else {
		lastScaleTime := ""
		if hpa.Status.LastScaleTime != nil {
			lastScaleTime = hpa.Status.LastScaleTime.String()
		}
		logger.V(4).Info("Decided not to scale",
			"scaleTarget", reference,
			"desiredReplicas", desiredReplicas,
			"lastScaleTime", lastScaleTime)
		desiredReplicas = currentReplicas
	}

	a.setStatus(hpa, currentReplicas, desiredReplicas, metricStatuses, rescale)

	// 上报期望副本数指标。
	a.monitor.ObserveDesiredReplicas(hpa.Namespace, hpa.Name, desiredReplicas)

	err = a.updateStatusIfNeeded(ctx, hpaStatusOriginal, hpa)
	if err != nil {
		// 此时可以直接覆盖 retErr，因为状态更新失败属于内部错误，优先级更高。
		return err
	}

	return retErr
}

// stabilizeRecommendation 对缩容建议做稳定化：
// - 用最新建议替换过期的历史建议；
// - 返回不早于 downscaleStabilisationWindow 的所有建议中的最大值。
//
// 取最大值意味着缩容会更保守：近期若有更高的建议，就先按较高的副本数维持。
func (a *HorizontalController) stabilizeRecommendation(key string, prenormalizedDesiredReplicas int32) int32 {
	maxRecommendation := prenormalizedDesiredReplicas
	foundOldSample := false
	oldSampleIndex := 0
	cutoff := time.Now().Add(-a.downscaleStabilisationWindow)

	a.recommendationsLock.Lock()
	defer a.recommendationsLock.Unlock()
	for i, rec := range a.recommendations[key] {
		if rec.timestamp.Before(cutoff) {
			// 复用已过期的槽位，避免历史建议列表无限增长。
			foundOldSample = true
			oldSampleIndex = i
		} else if rec.recommendation > maxRecommendation {
			maxRecommendation = rec.recommendation
		}
	}
	if foundOldSample {
		a.recommendations[key][oldSampleIndex] = timestampedRecommendation{prenormalizedDesiredReplicas, time.Now()}
	} else {
		a.recommendations[key] = append(a.recommendations[key], timestampedRecommendation{prenormalizedDesiredReplicas, time.Now()})
	}
	return maxRecommendation
}

// normalizeDesiredReplicas 对指标算出的期望副本数做归一化：
// 先按缩容稳定窗口修正，再钳制到 minReplicas/maxReplicas 范围内，
// 同时设置对应的 HPA 条件。
func (a *HorizontalController) normalizeDesiredReplicas(hpa *autoscalingv2.HorizontalPodAutoscaler, key string, currentReplicas int32, prenormalizedDesiredReplicas int32, minReplicas int32) int32 {
	stabilizedRecommendation := a.stabilizeRecommendation(key, prenormalizedDesiredReplicas)
	if stabilizedRecommendation != prenormalizedDesiredReplicas {
		// 稳定化改变了建议值，说明近期有更高的建议，记录在 AbleToScale 条件中。
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionTrue, "ScaleDownStabilized", "recent recommendations were higher than current one, applying the highest recent recommendation")
	} else {
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionTrue, "ReadyForNewScale", "recommended size matches current size")
	}

	desiredReplicas, reason, message := convertDesiredReplicasWithRules(currentReplicas, stabilizedRecommendation, minReplicas, hpa.Spec.MaxReplicas)

	if desiredReplicas == stabilizedRecommendation {
		setCondition(hpa, autoscalingv2.ScalingLimited, v1.ConditionFalse, reason, "%s", message)
	} else {
		setCondition(hpa, autoscalingv2.ScalingLimited, v1.ConditionTrue, reason, "%s", message)
	}

	return desiredReplicas
}

// NormalizationArg 把归一化所需的全部参数打包成一个结构，在多个函数间传递。
type NormalizationArg struct {
	// Key 是 HPA 的队列键，用于索引历史建议与扩缩容事件。
	Key string
	// ScaleUpBehavior 是 spec.behavior.scaleUp 中定义的扩容规则。
	ScaleUpBehavior *autoscalingv2.HPAScalingRules
	// ScaleDownBehavior 是 spec.behavior.scaleDown 中定义的缩容规则。
	ScaleDownBehavior *autoscalingv2.HPAScalingRules
	// MinReplicas 是允许的最小副本数。
	MinReplicas int32
	// MaxReplicas 是允许的最大副本数。
	MaxReplicas int32
	// CurrentReplicas 是当前副本数。
	CurrentReplicas int32
	// DesiredReplicas 是指标算出的期望副本数。
	DesiredReplicas int32
}

// normalizeDesiredReplicasWithBehaviors 按 behavior 规则归一化指标算出的期望副本数：
// 1. 应用基本约束（如不超过 maxReplicas、不低于 minReplicas 等）；
// 2. 应用 hpaSpec.Behaviors 中的单次扩缩容上限（如一次最多增加 4 个 Pod）；
// 3. 应用约束周期（如每分钟最多增加 4 个 Pod）；
// 4. 应用稳定窗口（如每分钟最多增加 4 个 Pod，且取最近 5 分钟内的最小建议）。
func (a *HorizontalController) normalizeDesiredReplicasWithBehaviors(hpa *autoscalingv2.HorizontalPodAutoscaler, key string, currentReplicas, prenormalizedDesiredReplicas, minReplicas int32) int32 {
	// 用户未指定缩容稳定窗口时用全局默认值补齐，保证后续代码无需处理 nil。
	a.maybeInitScaleDownStabilizationWindow(hpa)
	normalizationArg := NormalizationArg{
		Key:               key,
		ScaleUpBehavior:   hpa.Spec.Behavior.ScaleUp,
		ScaleDownBehavior: hpa.Spec.Behavior.ScaleDown,
		MinReplicas:       minReplicas,
		MaxReplicas:       hpa.Spec.MaxReplicas,
		CurrentReplicas:   currentReplicas,
		DesiredReplicas:   prenormalizedDesiredReplicas}
	stabilizedRecommendation, reason, message := a.stabilizeRecommendationWithBehaviors(normalizationArg)
	normalizationArg.DesiredReplicas = stabilizedRecommendation
	if stabilizedRecommendation != prenormalizedDesiredReplicas {
		// 根据扩缩容方向，原因会是 "ScaleUpStabilized" 或 "ScaleDownStabilized"。
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionTrue, reason, "%s", message)
	} else {
		setCondition(hpa, autoscalingv2.AbleToScale, v1.ConditionTrue, "ReadyForNewScale", "recommended size matches current size")
	}
	desiredReplicas, reason, message := a.convertDesiredReplicasWithBehaviorRate(normalizationArg)
	if desiredReplicas == stabilizedRecommendation {
		setCondition(hpa, autoscalingv2.ScalingLimited, v1.ConditionFalse, reason, "%s", message)
	} else {
		setCondition(hpa, autoscalingv2.ScalingLimited, v1.ConditionTrue, reason, "%s", message)
	}

	return desiredReplicas
}

// maybeInitScaleDownStabilizationWindow 在用户未指定缩容稳定窗口时，
// 用控制器的默认 downscaleStabilisationWindow 填充 spec，避免后续解引用 nil。
func (a *HorizontalController) maybeInitScaleDownStabilizationWindow(hpa *autoscalingv2.HorizontalPodAutoscaler) {
	behavior := hpa.Spec.Behavior
	if behavior != nil && behavior.ScaleDown != nil && behavior.ScaleDown.StabilizationWindowSeconds == nil {
		stabilizationWindowSeconds := (int32)(a.downscaleStabilisationWindow.Seconds())
		hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds = &stabilizationWindowSeconds
	}
}

// getReplicasChangePerPeriod 统计指定时间周期内所有扩缩容事件的副本数变化总和。
func getReplicasChangePerPeriod(periodSeconds int32, scaleEvents []timestampedScaleEvent) int32 {
	period := time.Second * time.Duration(periodSeconds)
	cutoff := time.Now().Add(-period)
	var replicas int32
	for _, rec := range scaleEvents {
		// 只统计窗口内的事件，过期事件已不计入速率限制。
		if rec.timestamp.After(cutoff) {
			replicas += rec.replicaChange
		}
	}
	return replicas

}

// getUnableComputeReplicaCountCondition 记录失败事件并返回一个 ScalingActive=false 的条件，
// 说明 HPA 无法计算副本数及失败原因。
func (a *HorizontalController) getUnableComputeReplicaCountCondition(hpa runtime.Object, reason string, err error) (condition autoscalingv2.HorizontalPodAutoscalerCondition) {
	a.eventRecorder.Event(hpa, v1.EventTypeWarning, reason, err.Error())
	return autoscalingv2.HorizontalPodAutoscalerCondition{
		Type:    autoscalingv2.ScalingActive,
		Status:  v1.ConditionFalse,
		Reason:  reason,
		Message: fmt.Sprintf("the HPA was unable to compute the replica count: %v", err),
	}
}

// storeScaleEvent 记录一次扩缩容事件（新增或替换已过期的事件）。
// 可被替换的事件由 markScaleEventsOutdated 标记为 outdated，从而复用槽位避免列表无限增长。
func (a *HorizontalController) storeScaleEvent(behavior *autoscalingv2.HorizontalPodAutoscalerBehavior, key string, prevReplicas, newReplicas int32) {
	if behavior == nil {
		return // 未配置 behavior 时不会用到事件，无需记录
	}
	var oldSampleIndex int
	var longestPolicyPeriod int32
	foundOldSample := false
	if newReplicas > prevReplicas {
		longestPolicyPeriod = getLongestPolicyPeriod(behavior.ScaleUp)

		a.scaleUpEventsLock.Lock()
		defer a.scaleUpEventsLock.Unlock()
		// 先把超出最长策略周期的事件标记为过期，供新事件复用槽位。
		markScaleEventsOutdated(a.scaleUpEvents[key], longestPolicyPeriod)
		replicaChange := newReplicas - prevReplicas
		for i, event := range a.scaleUpEvents[key] {
			if event.outdated {
				foundOldSample = true
				oldSampleIndex = i
			}
		}
		newEvent := timestampedScaleEvent{replicaChange, time.Now(), false}
		if foundOldSample {
			a.scaleUpEvents[key][oldSampleIndex] = newEvent
		} else {
			a.scaleUpEvents[key] = append(a.scaleUpEvents[key], newEvent)
		}
	} else {
		longestPolicyPeriod = getLongestPolicyPeriod(behavior.ScaleDown)

		a.scaleDownEventsLock.Lock()
		defer a.scaleDownEventsLock.Unlock()
		markScaleEventsOutdated(a.scaleDownEvents[key], longestPolicyPeriod)
		replicaChange := prevReplicas - newReplicas
		for i, event := range a.scaleDownEvents[key] {
			if event.outdated {
				foundOldSample = true
				oldSampleIndex = i
			}
		}
		newEvent := timestampedScaleEvent{replicaChange, time.Now(), false}
		if foundOldSample {
			a.scaleDownEvents[key][oldSampleIndex] = newEvent
		} else {
			a.scaleDownEvents[key] = append(a.scaleDownEvents[key], newEvent)
		}
	}
}

// stabilizeRecommendationWithBehaviors 按 behavior 配置的稳定窗口对建议值做稳定化：
// - 用最新建议替换过期的历史建议；
// - 返回未超出 ScaleUp/ScaleDown 稳定窗口的建议值中的最小值（扩容）或最大值（缩容）。
func (a *HorizontalController) stabilizeRecommendationWithBehaviors(args NormalizationArg) (int32, string, string) {
	now := time.Now()

	foundOldSample := false
	oldSampleIndex := 0

	upRecommendation := args.DesiredReplicas
	upDelaySeconds := *args.ScaleUpBehavior.StabilizationWindowSeconds
	upCutoff := now.Add(-time.Second * time.Duration(upDelaySeconds))

	downRecommendation := args.DesiredReplicas
	downDelaySeconds := *args.ScaleDownBehavior.StabilizationWindowSeconds
	downCutoff := now.Add(-time.Second * time.Duration(downDelaySeconds))

	// 计算稳定化的上下界：扩容取最近建议的最小值，缩容取最近建议的最大值。
	a.recommendationsLock.Lock()
	defer a.recommendationsLock.Unlock()
	for i, rec := range a.recommendations[args.Key] {
		if rec.timestamp.After(upCutoff) {
			upRecommendation = min(rec.recommendation, upRecommendation)
		}
		if rec.timestamp.After(downCutoff) {
			downRecommendation = max(rec.recommendation, downRecommendation)
		}
		if rec.timestamp.Before(upCutoff) && rec.timestamp.Before(downCutoff) {
			foundOldSample = true
			oldSampleIndex = i
		}
	}

	// 将建议值收敛到上下界之内，完成稳定化。
	recommendation := args.CurrentReplicas
	if recommendation < upRecommendation {
		recommendation = upRecommendation
	}
	if recommendation > downRecommendation {
		recommendation = downRecommendation
	}

	// 记录未经稳定化的原始建议，供后续调谐使用。
	if foundOldSample {
		a.recommendations[args.Key][oldSampleIndex] = timestampedRecommendation{args.DesiredReplicas, time.Now()}
	} else {
		a.recommendations[args.Key] = append(a.recommendations[args.Key], timestampedRecommendation{args.DesiredReplicas, time.Now()})
	}

	// 生成便于用户理解的原因与消息。
	var reason, message string
	if args.DesiredReplicas >= args.CurrentReplicas {
		reason = "ScaleUpStabilized"
		message = "recent recommendations were lower than current one, applying the lowest recent recommendation"
	} else {
		reason = "ScaleDownStabilized"
		message = "recent recommendations were higher than current one, applying the highest recent recommendation"
	}
	return recommendation, reason, message
}

// convertDesiredReplicasWithBehaviorRate 按 behavior 配置的速率上限规整期望副本数。
// 稳定窗口不在本函数中处理，而是单独处理。
func (a *HorizontalController) convertDesiredReplicasWithBehaviorRate(args NormalizationArg) (int32, string, string) {
	var possibleLimitingReason, possibleLimitingMessage string

	if args.DesiredReplicas > args.CurrentReplicas {
		a.scaleUpEventsLock.RLock()
		defer a.scaleUpEventsLock.RUnlock()
		a.scaleDownEventsLock.RLock()
		defer a.scaleDownEventsLock.RUnlock()
		scaleUpLimit := calculateScaleUpLimitWithScalingRules(args.CurrentReplicas, a.scaleUpEvents[args.Key], a.scaleDownEvents[args.Key], args.ScaleUpBehavior)

		if scaleUpLimit < args.CurrentReplicas {
			// 在扩容事件过期被清理前，不应继续调整。
			scaleUpLimit = args.CurrentReplicas
		}
		maximumAllowedReplicas := args.MaxReplicas
		if maximumAllowedReplicas > scaleUpLimit {
			maximumAllowedReplicas = scaleUpLimit
			possibleLimitingReason = "ScaleUpLimit"
			possibleLimitingMessage = "the desired replica count is increasing faster than the maximum scale rate"
		} else {
			possibleLimitingReason = "TooManyReplicas"
			possibleLimitingMessage = "the desired replica count is more than the maximum replica count"
		}
		if args.DesiredReplicas > maximumAllowedReplicas {
			return maximumAllowedReplicas, possibleLimitingReason, possibleLimitingMessage
		}
	} else if args.DesiredReplicas < args.CurrentReplicas {
		a.scaleUpEventsLock.RLock()
		defer a.scaleUpEventsLock.RUnlock()
		a.scaleDownEventsLock.RLock()
		defer a.scaleDownEventsLock.RUnlock()
		scaleDownLimit := calculateScaleDownLimitWithBehaviors(args.CurrentReplicas, a.scaleUpEvents[args.Key], a.scaleDownEvents[args.Key], args.ScaleDownBehavior)

		if scaleDownLimit > args.CurrentReplicas {
			// 在缩容事件过期被清理前，不应继续调整。
			scaleDownLimit = args.CurrentReplicas
		}
		minimumAllowedReplicas := args.MinReplicas
		if minimumAllowedReplicas < scaleDownLimit {
			minimumAllowedReplicas = scaleDownLimit
			possibleLimitingReason = "ScaleDownLimit"
			possibleLimitingMessage = "the desired replica count is decreasing faster than the maximum scale rate"
		} else {
			possibleLimitingMessage = "the desired replica count is less than the minimum replica count"
			possibleLimitingReason = "TooFewReplicas"
		}
		if args.DesiredReplicas < minimumAllowedReplicas {
			return minimumAllowedReplicas, possibleLimitingReason, possibleLimitingMessage
		}
	}
	return args.DesiredReplicas, "DesiredWithinRange", "the desired count is within the acceptable range"
}

// convertDesiredReplicasWithRules 按 min/max 副本数与固定扩容倍率上限规整期望副本数，
// 不依赖 HorizontalController 或 HorizontalPodAutoscaler 对象。
func convertDesiredReplicasWithRules(currentReplicas, desiredReplicas, hpaMinReplicas, hpaMaxReplicas int32) (int32, string, string) {

	var minimumAllowedReplicas int32
	var maximumAllowedReplicas int32

	var possibleLimitingCondition string
	var possibleLimitingReason string

	minimumAllowedReplicas = hpaMinReplicas

	// 限制单次扩容幅度，避免 heapster/kubelet 上报错误的 CPU 使用率时
	// 导致 master 副本数被不正确地快速拉高（见 issue #32304）。
	scaleUpLimit := calculateScaleUpLimit(currentReplicas)

	if hpaMaxReplicas > scaleUpLimit {
		maximumAllowedReplicas = scaleUpLimit
		possibleLimitingCondition = "ScaleUpLimit"
		possibleLimitingReason = "the desired replica count is increasing faster than the maximum scale rate"
	} else {
		maximumAllowedReplicas = hpaMaxReplicas
		possibleLimitingCondition = "TooManyReplicas"
		possibleLimitingReason = "the desired replica count is more than the maximum replica count"
	}

	if desiredReplicas < minimumAllowedReplicas {
		possibleLimitingCondition = "TooFewReplicas"
		possibleLimitingReason = "the desired replica count is less than the minimum replica count"

		return minimumAllowedReplicas, possibleLimitingCondition, possibleLimitingReason
	} else if desiredReplicas > maximumAllowedReplicas {
		return maximumAllowedReplicas, possibleLimitingCondition, possibleLimitingReason
	}

	return desiredReplicas, "DesiredWithinRange", "the desired count is within the acceptable range"
}

// calculateScaleUpLimit 计算默认的最大扩容目标副本数：
// 取当前副本数的 scaleUpLimitFactor 倍与 scaleUpLimitMinimum 中的较大者。
func calculateScaleUpLimit(currentReplicas int32) int32 {
	return int32(math.Max(scaleUpLimitFactor*float64(currentReplicas), scaleUpLimitMinimum))
}

// markScaleEventsOutdated 把超出最长策略周期、不再被任何 HPA 使用的扩缩容事件标记为过期，以便复用槽位。
func markScaleEventsOutdated(scaleEvents []timestampedScaleEvent, longestPolicyPeriod int32) {
	period := time.Second * time.Duration(longestPolicyPeriod)
	cutoff := time.Now().Add(-period)
	for i, event := range scaleEvents {
		if event.timestamp.Before(cutoff) {
			// 标记为过期以便后续复用该槽位。
			scaleEvents[i].outdated = true
		}
	}
}

// getLongestPolicyPeriod 返回规则集中最长的策略周期，
// 用于判断哪些历史扩缩容事件已经不可能再被任何策略使用。
func getLongestPolicyPeriod(scalingRules *autoscalingv2.HPAScalingRules) int32 {
	var longestPolicyPeriod int32
	for _, policy := range scalingRules.Policies {
		// 只有覆盖最长周期，才能安全地回收过期事件槽位。
		if policy.PeriodSeconds > longestPolicyPeriod {
			longestPolicyPeriod = policy.PeriodSeconds
		}
	}
	return longestPolicyPeriod
}

// calculateScaleUpLimitWithScalingRules 按给定 HPAScalingRules 计算单次最多可增加的 Pod 数。
func calculateScaleUpLimitWithScalingRules(currentReplicas int32, scaleUpEvents, scaleDownEvents []timestampedScaleEvent, scalingRules *autoscalingv2.HPAScalingRules) int32 {
	var result int32
	var proposed int32
	var selectPolicyFn func(int32, int32) int32
	if *scalingRules.SelectPolicy == autoscalingv2.DisabledPolicySelect {
		return currentReplicas // 扩缩容被禁用。
	} else if *scalingRules.SelectPolicy == autoscalingv2.MinChangePolicySelect {
		result = math.MaxInt32
		selectPolicyFn = minInt32 // 扩容时，“最小变化”策略产生最小值。
	} else {
		result = math.MinInt32
		selectPolicyFn = maxInt32 // 其他情况使用默认策略，取最大可能变化。
	}
	for _, policy := range scalingRules.Policies {
		replicasAddedInCurrentPeriod := getReplicasChangePerPeriod(policy.PeriodSeconds, scaleUpEvents)
		replicasDeletedInCurrentPeriod := getReplicasChangePerPeriod(policy.PeriodSeconds, scaleDownEvents)
		periodStartReplicas := currentReplicas - replicasAddedInCurrentPeriod + replicasDeletedInCurrentPeriod
		if policy.Type == autoscalingv2.PodsScalingPolicy {
			proposed = periodStartReplicas + policy.Value
		} else if policy.Type == autoscalingv2.PercentScalingPolicy {
			// 必须向上取整，否则变化量可能不足 1 个副本，导致目标永远无法扩容。
			proposed = int32(math.Ceil(float64(periodStartReplicas) * (1 + float64(policy.Value)/100)))
		}
		result = selectPolicyFn(result, proposed)
	}
	return result
}

// calculateScaleDownLimitWithBehaviors 按给定 HPAScalingRules 计算单次最多可删除的 Pod 数。
func calculateScaleDownLimitWithBehaviors(currentReplicas int32, scaleUpEvents, scaleDownEvents []timestampedScaleEvent, scalingRules *autoscalingv2.HPAScalingRules) int32 {
	var result int32
	var proposed int32
	var selectPolicyFn func(int32, int32) int32
	if *scalingRules.SelectPolicy == autoscalingv2.DisabledPolicySelect {
		return currentReplicas // 扩缩容被禁用。
	} else if *scalingRules.SelectPolicy == autoscalingv2.MinChangePolicySelect {
		result = math.MinInt32
		selectPolicyFn = maxInt32 // 缩容时，“最小变化”策略对应最大的副本数结果。
	} else {
		result = math.MaxInt32
		selectPolicyFn = minInt32 // 其他情况使用默认策略，取最小副本数结果。
	}
	for _, policy := range scalingRules.Policies {
		replicasAddedInCurrentPeriod := getReplicasChangePerPeriod(policy.PeriodSeconds, scaleUpEvents)
		replicasDeletedInCurrentPeriod := getReplicasChangePerPeriod(policy.PeriodSeconds, scaleDownEvents)
		periodStartReplicas := currentReplicas - replicasAddedInCurrentPeriod + replicasDeletedInCurrentPeriod
		if policy.Type == autoscalingv2.PodsScalingPolicy {
			proposed = periodStartReplicas - policy.Value
		} else if policy.Type == autoscalingv2.PercentScalingPolicy {
			proposed = int32(float64(periodStartReplicas) * (1 - float64(policy.Value)/100))
		}
		result = selectPolicyFn(result, proposed)
	}
	return result
}

// scaleForResourceMappings 尝试按给定名称与命名空间获取资源的 scale 子资源，
// 依次尝试每个 RESTMapping 直到成功；若全部失败则返回第一个错误。
// 成功时同时返回 scale 对象与生效映射的 group-resource。
func (a *HorizontalController) scaleForResourceMappings(ctx context.Context, namespace, name string, mappings []*apimeta.RESTMapping) (*autoscalingv1.Scale, schema.GroupResource, error) {
	var firstErr error
	for i, mapping := range mappings {
		targetGR := mapping.Resource.GroupResource()
		scale, err := a.scaleNamespacer.Scales(namespace).Get(ctx, targetGR, name, metav1.GetOptions{})
		if err == nil {
			return scale, targetGR, nil
		}

		// 记住第一个错误，然后继续尝试其他映射，直到找到可用的为止。
		if i == 0 {
			firstErr = err
		}
	}

	// 处理映射集合为空的情况。
	if firstErr == nil {
		firstErr = fmt.Errorf("unrecognized resource")
	}

	return nil, schema.GroupResource{}, firstErr
}

// setCurrentReplicasAndMetricsInStatus 把当前副本数与指标状态写入 HPA，期望副本数保持不变。
func (a *HorizontalController) setCurrentReplicasAndMetricsInStatus(hpa *autoscalingv2.HorizontalPodAutoscaler, currentReplicas int32, metricStatuses []autoscalingv2.MetricStatus) {
	a.setStatus(hpa, currentReplicas, hpa.Status.DesiredReplicas, metricStatuses, false)
}

// setStatus 重建给定 HPA 的状态，更新当前/期望副本数以及指标状态。
func (a *HorizontalController) setStatus(hpa *autoscalingv2.HorizontalPodAutoscaler, currentReplicas, desiredReplicas int32, metricStatuses []autoscalingv2.MetricStatus, rescale bool) {
	hpa.Status = autoscalingv2.HorizontalPodAutoscalerStatus{
		CurrentReplicas: currentReplicas,
		DesiredReplicas: desiredReplicas,
		LastScaleTime:   hpa.Status.LastScaleTime,
		CurrentMetrics:  metricStatuses,
		Conditions:      hpa.Status.Conditions,
	}

	statusObservedGeneration := hpa.Generation
	if utilfeature.DefaultFeatureGate.Enabled(features.HPAGeneration) {
		hpa.Status.ObservedGeneration = &statusObservedGeneration
	}
	if rescale {
		now := metav1.NewTime(time.Now())
		hpa.Status.LastScaleTime = &now
	}
}

// updateStatusIfNeeded 仅在新旧状态不一致时才调用 updateStatus，避免无谓的 API 写操作。
func (a *HorizontalController) updateStatusIfNeeded(ctx context.Context, oldStatus *autoscalingv2.HorizontalPodAutoscalerStatus, newHPA *autoscalingv2.HorizontalPodAutoscaler) error {
	// 状态没有变化就跳过写入。
	if apiequality.Semantic.DeepEqual(oldStatus, &newHPA.Status) {
		return nil
	}
	return a.updateStatus(ctx, newHPA)
}

// updateStatus 实际发起 HPA 状态更新请求，并记录一致性状态。
func (a *HorizontalController) updateStatus(ctx context.Context, hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	updatedHPA, err := a.hpaNamespacer.HorizontalPodAutoscalers(hpa.Namespace).UpdateStatus(ctx, hpa, metav1.UpdateOptions{})
	if err != nil {
		a.eventRecorder.Event(hpa, v1.EventTypeWarning, "FailedUpdateStatus", err.Error())
		return fmt.Errorf("failed to update status for %s: %w", hpa.Name, err)
	}
	a.consistencyStore.WroteAt(
		types.NamespacedName{Namespace: updatedHPA.Namespace, Name: updatedHPA.Name},
		updatedHPA.UID,
		horizontalGroupResource,
		updatedHPA.ResourceVersion,
	)
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Successfully updated status", "HPA", klog.KObj(hpa))
	return nil
}

// tolerancesForHpa 返回给定 HPA 的指标使用率容差。
// 若 HPAConfigurableTolerance 特性门控关闭，则忽略 spec.behavior 中配置的可变容差。
func (a *HorizontalController) tolerancesForHpa(hpa *autoscalingv2.HorizontalPodAutoscaler) Tolerances {
	t := Tolerances{a.tolerance, a.tolerance}
	behavior := hpa.Spec.Behavior
	allowConfigurableTolerances := utilfeature.DefaultFeatureGate.Enabled(features.HPAConfigurableTolerance)
	if behavior == nil || !allowConfigurableTolerances {
		return t
	}
	if behavior.ScaleDown != nil && behavior.ScaleDown.Tolerance != nil {
		t.scaleDown = behavior.ScaleDown.Tolerance.AsApproximateFloat64()
	}
	if behavior.ScaleUp != nil && behavior.ScaleUp.Tolerance != nil {
		t.scaleUp = behavior.ScaleUp.Tolerance.AsApproximateFloat64()
	}
	return t
}

// setCondition 把 HPA 上指定类型的条件设置为给定状态、原因与消息。
// message 与 args 按格式化字符串处理；条件不存在时会自动添加。
func setCondition(hpa *autoscalingv2.HorizontalPodAutoscaler, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType, status v1.ConditionStatus, reason, message string, args ...interface{}) {
	hpa.Status.Conditions = setConditionInList(hpa.Status.Conditions, conditionType, status, reason, message, args...)
}

// removeCondition 从 HPA 条件列表中移除指定类型的条件。
func removeCondition(hpa *autoscalingv2.HorizontalPodAutoscaler, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType) {
	// 原地过滤，复用底层数组以避免额外分配。
	filtered := hpa.Status.Conditions[:0]
	for _, c := range hpa.Status.Conditions {
		if c.Type != conditionType {
			filtered = append(filtered, c)
		}
	}
	hpa.Status.Conditions = filtered
}

// hasObjectOrExternalMetrics 判断 HPA 是否至少包含一条 object 或 external 类型的指标。
func hasObjectOrExternalMetrics(hpa *autoscalingv2.HorizontalPodAutoscaler) bool {
	for _, metric := range hpa.Spec.Metrics {
		if metric.Type == autoscalingv2.ObjectMetricSourceType || metric.Type == autoscalingv2.ExternalMetricSourceType {
			return true
		}
	}
	return false
}

// getScaledToZeroConditionStatus 在 ScaledToZero 条件存在且为 True 时返回 true。
func getScaledToZeroConditionStatus(hpa *autoscalingv2.HorizontalPodAutoscaler) bool {
	for _, condition := range hpa.Status.Conditions {
		if condition.Type == autoscalingv2.ScaledToZero {
			return condition.Status == v1.ConditionTrue
		}
	}
	return false
}

// setConditionInList 把条件列表中指定类型的条件设置为给定状态、原因与消息。
// message 与 args 按格式化字符串处理；条件不存在时会添加。状态变化时更新 LastTransitionTime，返回新的条件列表。
func setConditionInList(inputList []autoscalingv2.HorizontalPodAutoscalerCondition, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType, status v1.ConditionStatus, reason, message string, args ...interface{}) []autoscalingv2.HorizontalPodAutoscalerCondition {
	resList := inputList
	var existingCond *autoscalingv2.HorizontalPodAutoscalerCondition
	for i, condition := range resList {
		if condition.Type == conditionType {
			// 不能对 range 的迭代变量取地址，因此取切片元素的地址。
			existingCond = &resList[i]
			break
		}
	}

	if existingCond == nil {
		resList = append(resList, autoscalingv2.HorizontalPodAutoscalerCondition{
			Type: conditionType,
		})
		existingCond = &resList[len(resList)-1]
	}

	if existingCond.Status != status {
		existingCond.LastTransitionTime = metav1.Now()
	}

	existingCond.Status = status
	existingCond.Reason = reason
	existingCond.Message = fmt.Sprintf(message, args...)

	return resList
}

// minInt32 是 min 内建函数的包装，便于作为函数值传递。
func minInt32(a, b int32) int32 {
	return min(a, b)
}

// maxInt32 是 max 内建函数的包装，便于作为函数值传递。
func maxInt32(a, b int32) int32 {
	return max(a, b)
}
