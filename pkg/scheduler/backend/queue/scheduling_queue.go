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

// 本文件包含实现调度队列的结构体。
// 调度队列存放等待调度的实体（可以是 Pod 或 PodGroup）。这里实现的是一个优先级队列，
// 由两个子队列和一个额外的数据结构组成：activeQ、backoffQ 和 unschedulableEntities。
// - activeQ 存放正在被考虑调度的实体。
// - backoffQ 存放从 unschedulableEntities 移出的实体，退避期结束后再移入 activeQ。
// - unschedulableEntities 存放已尝试过调度、但当前被判定为不可调度的实体。

package queue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/backend/heap"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/clock"
)

const (
	// DefaultPodMaxInUnschedulablePodsDuration 是 Pod 可在 unschedulableEntities 中停留的最长时间默认值。
	// 超过该时间后，Pod 会从 unschedulableEntities 移入 backoffQ 或 activeQ。
	// 若该值为空，则使用默认值（5min）。
	DefaultPodMaxInUnschedulablePodsDuration time.Duration = 5 * time.Minute
	// 调度队列名称
	activeQ        = "Active"
	backoffQ       = "Backoff"
	unschedulableQ = "Unschedulable"

	preEnqueue = "PreEnqueue"
)

const (
	// DefaultPodInitialBackoffDuration 是不可调度 Pod 的初始退避时长默认值。
	// 要修改调度器使用的默认 podInitialBackoffDurationSeconds，请更新 defaults.go 中的 ComponentConfig 值。
	DefaultPodInitialBackoffDuration time.Duration = 1 * time.Second
	// DefaultPodMaxBackoffDuration 是不可调度 Pod 的最大退避时长默认值。
	// 要修改调度器使用的默认 podMaxBackoffDurationSeconds，请更新 defaults.go 中的 ComponentConfig 值。
	DefaultPodMaxBackoffDuration time.Duration = 10 * time.Second
)

// PreEnqueueCheck 是一个函数类型，用于构造对 Pod 进行检查的函数，
// 调用方根据检查结果决定将该 Pod 入队还是跳过。
type PreEnqueueCheck func(pod *v1.Pod) bool

// PodSigner 为 Pod 创建代表其调度需求的调度签名。
// 该签名被机会批处理特性（KEP-5598）用于复用调度决策。
// 若 Pod "不可签名"（某些插件无法为其生成稳定签名），返回 nil。
type PodSigner func(ctx context.Context, pod *v1.Pod) fwk.PodSignature

// SchedulingQueue 是存放待调度 Pod 的队列接口。
// 该接口借鉴了 cache.FIFO 和 cache.Heap 的模式，便于用这些数据结构实现 SchedulingQueue。
type SchedulingQueue interface {
	fwk.PodNominator
	// Add 将新建的 Pod 加入调度队列。
	Add(ctx context.Context, pod *v1.Pod)
	// Activate 将给定的 Pod 移入 activeQ。
	// 若 Pod 不在 unschedulableEntities 或 backoffQ 中且处于 in-flight 状态，
	// 则注册通配事件，待其返回队列时重新入队。
	// 若 Pod 不在上述两处且不处于 in-flight 状态（即完全未知的 Pod），Activate 会忽略它。
	Activate(logger klog.Logger, pods map[string]*v1.Pod)
	// AddUnschedulablePodIfNotPresent 将不可调度的 Pod 加回调度队列（若尚未在队列中）。
	// podSchedulingCycle 表示当前调度周期号，可通过 SchedulingCycle() 获取。
	AddUnschedulablePodIfNotPresent(logger klog.Logger, pInfo *framework.QueuedPodInfo, podSchedulingCycle int64) error
	// AddAttemptedPodGroupIfNeeded 将已尝试调度的 PodGroup 加回调度队列。
	// 若没有待处理 Pod，则不会把该 PodGroup 加回队列。
	// 应与 PodGroup 调度周期同步调用。
	// 若 PodGroup 调度成功，其余 Pod 将直接重新入 activeQ，而不是走退避。
	// 若仍有不可调度的 Pod，PodGroup 会保留其时间戳，以便在当前周期后立即触发抢占，
	// 除非期间来了更高优先级的 Pod 或 PodGroup。
	AddAttemptedPodGroupIfNeeded(logger klog.Logger, pgInfo *framework.QueuedPodGroupInfo, schedulingCycle int64, status *fwk.Status) error
	// SchedulingCycle 返回调度队列缓存的当前调度周期号。
	// 通常每次 Pop 出一个 Pod 时递增该值即可。
	SchedulingCycle() int64
	// Pop 移除并返回队首实体。队列为空时阻塞，直到有新实体入队。
	Pop(logger klog.Logger) (framework.QueuedEntityInfo, error)
	// 对 Pop 返回的 Pod 必须调用 Done，以便队列跟踪当前正在处理的 Pod。
	Done(types.UID)
	// Update 更新队列中的 Pod；若 Pod 不在任何队列中，则将其加入 activeQ。
	Update(ctx context.Context, oldPod, newPod *v1.Pod)
	// Delete 从队列中删除 Pod。
	Delete(logger klog.Logger, pod *v1.Pod)
	// 重要提示：preCheck 不应包含任何依赖 in-tree 插件逻辑的内容
	// （例如根据新增/更新 Node 的容量过滤 Pod）。
	MoveAllToActiveOrBackoffQueue(logger klog.Logger, event fwk.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck)
	// AddPodGroup 向队列新增一个 PodGroup 对象，
	// 并将与该 PodGroup 关联的所有 Pod 重新入队。
	AddPodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup)
	// UpdatePodGroup 更新队列中已有的 PodGroup 对象。
	UpdatePodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup)
	// DeletePodGroup 从队列移除一个 PodGroup 对象，
	// 并将与该 PodGroup 关联的所有 Pod 移入 incompletePodGroupPods。
	DeletePodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup)

	// Close 关闭 SchedulingQueue，使等待 Pop 的 goroutine 能正常退出。
	Close()
	// Run 启动管理队列的 goroutine。
	Run(logger klog.Logger)

	// PatchPodStatus 通过 API dispatcher 发送更新 API 调用来处理 Pod 状态更新。
	// 仅在 SchedulerAsyncAPICalls feature gate 启用时才应使用该方法。
	PatchPodStatus(pod *v1.Pod, conditions []*v1.PodCondition, nominatingInfo *fwk.NominatingInfo) (<-chan error, error)

	// 以下函数仅用于测试或调试。
	GetPodGroup(name, namespace string, entityKeyType fwk.EntityKeyType) (*framework.QueuedPodGroupInfo, bool)
	GetPod(name, namespace string, schedulingGroup *v1.PodSchedulingGroup) (*framework.QueuedPodInfo, bool)
	PendingPods() ([]*v1.Pod, string)
	InFlightPods() []*v1.Pod
	PodsInActiveQ() []*v1.Pod
	// PodsInBackoffQ 返回 backoffQ 中的所有 Pod。
	PodsInBackoffQ() []*v1.Pod
	UnschedulablePods() []*v1.Pod
	// pendingPodGroupPods 存放所有等待其所属 PodGroup 重新入队的待处理 Pod。
	PendingPodGroupPods() []*v1.Pod
	// IncompletePodGroupPodsPods 返回所有属于 PodGroup、但仍在等待其 API 对象（PodGroup）被观察到的 Pod。
	IncompletePodGroupPodsPods() []*v1.Pod

	// AddCompositePodGroup 向队列新增一个组合 PodGroup。
	AddCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup)
	// UpdateCompositePodGroup 更新队列中的组合 PodGroup。
	UpdateCompositePodGroup(logger klog.Logger, newCPG *schedulingv1alpha3.CompositePodGroup)
	// DeleteCompositePodGroup 从队列移除一个组合 PodGroup。
	DeleteCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup)
}

// NewSchedulingQueue 初始化一个优先级队列作为新的调度队列。
func NewSchedulingQueue(
	lessFn fwk.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option) SchedulingQueue {
	return NewPriorityQueue(lessFn, informerFactory, opts...)
}

// PriorityQueue 实现了调度队列。
// 队首是优先级最高的待调度实体（Pod 或 PodGroup）。该结构包含两个子队列和一个额外的数据结构：
// activeQ、backoffQ 和 unschedulableEntities。
//   - activeQ 存放正在被考虑调度的 Pod 或 PodGroup。
//   - backoffQ 存放从 unschedulableEntities 移出的 Pod 或 PodGroup，退避期结束后再移入 activeQ。
//   - unschedulableEntities 存放已尝试过调度、但当前被判定为不可调度的 Pod 或 PodGroup。
//
// 为处理 PodGroup，PriorityQueue 还扩展了以下结构：
//   - incompletePodGroupPods：存放等待对应 PodGroup 对象被观察到的 Pod 信息。
//   - pendingPodGroupPods：存放所有等待其所属 in-flight PodGroup 重新入队的待处理 Pod。
//   - workloadForest：维护已观察到的 PodGroup 对象的一致视图。
//
// 这些额外结构确保 Pod 及其所属 PodGroup 满足以下生命周期不变式：
//  1. 未完成（Incomplete）：当且仅当 Pod 的根 PodGroup 不在 `workloadForest` 中时，
//     Pod 被放入 `incompletePodGroupPods`。
//  2. 已入队（Queued）：否则，若 PodGroup 当前不在 in-flight 状态，Pod 会被封装进 QueuedPodGroupInfo，
//     存放于 `activeQ`、`backoffQ` 或 `unschedulableEntities` 中。
//  3. 处理中（In-flight）：若 PodGroup 当前处于 in-flight 状态（已从队列弹出并正在评估），
//     新到达的 Pod 会被放入 `pendingPodGroupPods`，等待该 PodGroup 重新入队。
type PriorityQueue struct {
	*nominator

	stop  chan struct{}
	clock clock.WithTicker

	// lock 优先级最高，必须先于队列中任何其他锁获取
	// （activeQueue.lock、backoffQueue.lock 或 nominator.nLock）。
	// 正确的加锁顺序为：lock > activeQueue.lock > backoffQueue.lock > nominator.nLock。
	lock sync.RWMutex

	// Pod 可在 unschedulableEntities 中停留的最长时间。
	podMaxInUnschedulablePodsDuration time.Duration

	activeQ  activeQueuer
	backoffQ backoffQueuer
	// unschedulableEntities 存放已尝试过调度、但被判定为不可调度的 Pod 和 PodGroup。
	unschedulableEntities *unschedulableEntities
	// pendingPodGroupPods 存放所有等待其所属 PodGroup 重新入队的待处理 Pod。
	// 仅在 GenericWorkload feature 启用时使用。
	pendingPodGroupPods *podGroupMemberPods
	// incompletePodGroupPods 存放等待对应 PodGroup 对象被观察到的 Pod 信息。
	// 仅在 GenericWorkload feature 启用时使用。
	incompletePodGroupPods *podGroupMemberPods

	// workloadForest 维护已观察到的 PodGroup 对象的一致视图。
	// 它确保调度队列的不变式不受 scheduler cache 异步更新的影响。
	// 调度队列之外应以 cache 为准。
	// 仅在 GenericWorkload feature 启用时使用。
	workloadForest *workloadForest

	// preEnqueuePluginMap 以 profile 名和插件名为键，值为已注册的 preEnqueue 插件。
	preEnqueuePluginMap map[string]map[string]fwk.PreEnqueuePlugin
	// queueingHintMap 以 profile 名为键，值为已注册的 queueing hint 函数。
	queueingHintMap QueueingHintMapPerProfile
	// pluginToEventsMap 记录各插件感兴趣的事件。
	pluginToEventsMap map[string][]fwk.ClusterEvent

	nsLister listersv1.NamespaceLister

	metricsRecorder *metrics.MetricAsyncRecorder
	// pluginMetricsSamplePercent 是插件指标采样的百分比。
	pluginMetricsSamplePercent int

	// apiDispatcher 用于需要发送 API 调用的方法。
	// 仅当 SchedulerAsyncAPICalls feature gate 启用时非 nil。
	apiDispatcher fwk.APIDispatcher

	// podSigners 将 profile 名映射到该 profile 的签名函数。
	podSigners map[string]PodSigner

	// isPopFromBackoffQEnabled 表示 SchedulerPopFromBackoffQ feature gate 是否启用。
	isPopFromBackoffQEnabled bool
	// isGenericWorkloadEnabled 表示 GenericWorkload feature gate 是否启用。
	isGenericWorkloadEnabled bool
	// isOpportunisticBatchingEnabled 表示 OpportunisticBatching feature gate 是否启用。
	isOpportunisticBatchingEnabled bool
	// isCompositePodGroupEnabled 表示 CompositePodGroup 的 feature gate 是否启用。
	isCompositePodGroupEnabled bool
	// isInPlacePodVerticalScalingSchedulerPreemptionEnabled 表示 InPlacePodVerticalScalingSchedulerPreemption feature gate 是否启用。
	isInPlacePodVerticalScalingSchedulerPreemptionEnabled bool
	// isPreQueueingHintsEnabled 表示 SchedulerPreQueueingHints feature gate 是否启用。
	isPreQueueingHintsEnabled bool
}

// QueueingHintFunction 是对 QueueingHintFn 的包装，附带 PluginName。
type QueueingHintFunction struct {
	PluginName        string
	QueueingHintFn    fwk.QueueingHintFn
	PreQueueingHintFn fwk.PreQueueingHintFn
}

// clusterEvent 包含事件及涉及的对象。
type clusterEvent struct {
	event fwk.ClusterEvent
	// oldObj 是涉及该事件的旧对象。
	oldObj interface{}
	// newObj 是涉及该事件的新对象。
	newObj interface{}
}

type priorityQueueOptions struct {
	clock                             clock.WithTicker
	podInitialBackoffDuration         time.Duration
	podMaxBackoffDuration             time.Duration
	podMaxInUnschedulablePodsDuration time.Duration
	podLister                         listersv1.PodLister
	metricsRecorder                   *metrics.MetricAsyncRecorder
	pluginMetricsSamplePercent        int
	preEnqueuePluginMap               map[string]map[string]fwk.PreEnqueuePlugin
	queueingHintMap                   QueueingHintMapPerProfile
	apiDispatcher                     fwk.APIDispatcher
	podSigners                        map[string]PodSigner
}

// Option 用于配置 PriorityQueue。
type Option func(*priorityQueueOptions)

// WithClock 为 PriorityQueue 设置时钟，默认使用 clock.RealClock。
func WithClock(clock clock.WithTicker) Option {
	return func(o *priorityQueueOptions) {
		o.clock = clock
	}
}

// WithPodInitialBackoffDuration 设置 PriorityQueue 中 Pod 的初始退避时长。
func WithPodInitialBackoffDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podInitialBackoffDuration = duration
	}
}

// WithPodMaxBackoffDuration 设置 PriorityQueue 中 Pod 的最大退避时长。
func WithPodMaxBackoffDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podMaxBackoffDuration = duration
	}
}

// WithPodLister 为 PriorityQueue 设置 Pod lister。
func WithPodLister(pl listersv1.PodLister) Option {
	return func(o *priorityQueueOptions) {
		o.podLister = pl
	}
}

// WithPodMaxInUnschedulablePodsDuration 设置 PriorityQueue 的 podMaxInUnschedulablePodsDuration。
func WithPodMaxInUnschedulablePodsDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podMaxInUnschedulablePodsDuration = duration
	}
}

// QueueingHintMapPerProfile 以 profile 名为键，值为该 profile 注册的 queueing hint map。
type QueueingHintMapPerProfile map[string]QueueingHintMap

// QueueingHintMap 以 ClusterEvent 为键，值为该事件注册的 queueing hint 函数。
type QueueingHintMap map[fwk.ClusterEvent][]*QueueingHintFunction

// WithQueueingHintMapPerProfile 为 PriorityQueue 设置 queueingHintMap。
func WithQueueingHintMapPerProfile(m QueueingHintMapPerProfile) Option {
	return func(o *priorityQueueOptions) {
		o.queueingHintMap = m
	}
}

// WithPreEnqueuePluginMap 为 PriorityQueue 设置 preEnqueuePluginMap。
func WithPreEnqueuePluginMap(m map[string]map[string]fwk.PreEnqueuePlugin) Option {
	return func(o *priorityQueueOptions) {
		o.preEnqueuePluginMap = m
	}
}

// WithMetricsRecorder 设置指标记录器。
func WithMetricsRecorder(recorder *metrics.MetricAsyncRecorder) Option {
	return func(o *priorityQueueOptions) {
		o.metricsRecorder = recorder
	}
}

// WithPluginMetricsSamplePercent 设置插件指标的采样百分比。
func WithPluginMetricsSamplePercent(percent int) Option {
	return func(o *priorityQueueOptions) {
		o.pluginMetricsSamplePercent = percent
	}
}

// WithAPIDispatcher 设置 API dispatcher。
func WithAPIDispatcher(apiDispatcher fwk.APIDispatcher) Option {
	return func(o *priorityQueueOptions) {
		o.apiDispatcher = apiDispatcher
	}
}

// WithPodSigners 按调度器 profile 设置 Pod 签名函数。
// 每个 profile 可以有自己的签名函数来计算 Pod 签名。
// Pod 签名让调度器能够缓存并复用相同 Pod 的过滤/打分结果，从而实现机会批处理（KEP-5598）。
func WithPodSigners(signers map[string]PodSigner) Option {
	return func(o *priorityQueueOptions) {
		o.podSigners = signers
	}
}

var defaultPriorityQueueOptions = priorityQueueOptions{
	clock:                             clock.RealClock{},
	podInitialBackoffDuration:         DefaultPodInitialBackoffDuration,
	podMaxBackoffDuration:             DefaultPodMaxBackoffDuration,
	podMaxInUnschedulablePodsDuration: DefaultPodMaxInUnschedulablePodsDuration,
}

// 确保 PriorityQueue 实现了 SchedulingQueue。
var _ SchedulingQueue = &PriorityQueue{}

// newQueuedPodInfoForLookup 构造一个仅用于在队列中查找的 QueuedPodInfo 对象。
func newQueuedPodInfoForLookup(pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	// 仅用于队列查找，只需设置 Pod，避免构造开销较大的完整 PodInfo（此处会被频繁调用）。

	return &framework.QueuedPodInfo{
		PodInfo: &framework.PodInfo{Pod: pod},
		QueueingParams: framework.QueueingParams{
			UnschedulablePlugins: sets.New(plugins...),
		},
	}
}

// newQueuedPodGroupInfoFromRootLookup 构造一个用于查找的 QueuedPodGroupInfo 对象。
// 假定给定的 rootLookup 确实是 workloadForest 中的一个根，因此理论上不会返回 nil。
func (p *PriorityQueue) newQueuedPodGroupInfoFromRootLookup(logger klog.Logger, pInfo *framework.QueuedPodInfo, rootLookup *framework.QueuedPodGroupInfo) *framework.QueuedPodGroupInfo {
	rootInfo := p.workloadForest.buildQueuedPodGroupInfo(logger, rootLookup)
	if rootInfo == nil {
		// rootLookup 保证是 workloadForest 中的根，理论上不应走到这里。
		return nil
	}
	rootInfo.AddPod(pInfo)
	rootInfo.QueueingParams.Timestamp = pInfo.GetTimestamp()
	rootInfo.QueueingParams.InitialAttemptTimestamp = pInfo.GetInitialAttemptTimestamp()
	return rootInfo
}

// NewPriorityQueue 创建一个 PriorityQueue 对象。
func NewPriorityQueue(
	lessFn fwk.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) *PriorityQueue {
	options := defaultPriorityQueueOptions
	if options.podLister == nil {
		options.podLister = informerFactory.Core().V1().Pods().Lister()
	}
	for _, opt := range opts {
		opt(&options)
	}

	isPopFromBackoffQEnabled := utilfeature.DefaultFeatureGate.Enabled(features.SchedulerPopFromBackoffQ)
	isGenericWorkloadEnabled := utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload)
	isOpportunisticBatchingEnabled := utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching)
	isCompositePodGroupEnabled := utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup)
	isInPlacePodVerticalScalingSchedulerPreemptionEnabled := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption)
	isPreQueueingHintsEnabled := utilfeature.DefaultFeatureGate.Enabled(features.SchedulerPreQueueingHints)
	lessConverted := convertLessFn(lessFn)

	backoffQ := newBackoffQueue(options.clock, options.podInitialBackoffDuration, options.podMaxBackoffDuration, lessConverted, isPopFromBackoffQEnabled)
	pq := &PriorityQueue{
		clock:                             options.clock,
		stop:                              make(chan struct{}),
		podMaxInUnschedulablePodsDuration: options.podMaxInUnschedulablePodsDuration,
		backoffQ:                          backoffQ,
		unschedulableEntities:             newUnschedulableEntities(metrics.NewUnschedulableEntitiesRecorder(), metrics.NewGatedEntitiesRecorder()),
		pendingPodGroupPods:               newPodGroupMemberPods(metrics.PendingPodGroupPods()),
		incompletePodGroupPods:            newPodGroupMemberPods(metrics.IncompletePodGroupPods()),
		workloadForest:                    newWorkloadForest(isCompositePodGroupEnabled),
		preEnqueuePluginMap:               options.preEnqueuePluginMap,
		queueingHintMap:                   options.queueingHintMap,
		pluginToEventsMap:                 buildEventMap(options.queueingHintMap),
		metricsRecorder:                   options.metricsRecorder,
		pluginMetricsSamplePercent:        options.pluginMetricsSamplePercent,
		apiDispatcher:                     options.apiDispatcher,
		podSigners:                        options.podSigners,
		isPopFromBackoffQEnabled:          isPopFromBackoffQEnabled,
		isGenericWorkloadEnabled:          isGenericWorkloadEnabled,
		isOpportunisticBatchingEnabled:    isOpportunisticBatchingEnabled,
		isCompositePodGroupEnabled:        isCompositePodGroupEnabled,
		isInPlacePodVerticalScalingSchedulerPreemptionEnabled: isInPlacePodVerticalScalingSchedulerPreemptionEnabled,
		isPreQueueingHintsEnabled:                             isPreQueueingHintsEnabled,
	}
	var backoffQPopper backoffQPopper
	if isPopFromBackoffQEnabled {
		backoffQPopper = backoffQ
	}
	pq.activeQ = newActiveQueue(heap.NewWithRecorder(queuedEntityKeyFunc, heap.LessFunc[framework.QueuedEntityInfo](lessConverted), metrics.NewActiveEntitiesRecorder()), options.metricsRecorder, backoffQPopper)
	pq.nsLister = informerFactory.Core().V1().Namespaces().Lister()
	pq.nominator = newPodNominator(options.podLister)

	return pq
}

// convertLessFn 包装 fwk.LessFunc，将其转换为接受 framework.QueuedEntityInfo 作为参数的函数。
func convertLessFn(lessFn fwk.LessFunc) func(entity1, entity2 framework.QueuedEntityInfo) bool {
	return func(entity1, entity2 framework.QueuedEntityInfo) bool {
		return lessFn(entity1.(fwk.QueuedEntityInfo), entity2.(fwk.QueuedEntityInfo))
	}
}

func buildEventMap(qHintMap QueueingHintMapPerProfile) map[string][]fwk.ClusterEvent {
	eventMap := make(map[string][]fwk.ClusterEvent)

	for _, hintMap := range qHintMap {
		for event, qHints := range hintMap {
			for _, qHint := range qHints {
				eventMap[qHint.PluginName] = append(eventMap[qHint.PluginName], event)
			}
		}
	}

	return eventMap
}

// Run 启动将实体从 backoffQ 搬运到 activeQ 的 goroutine。
func (p *PriorityQueue) Run(logger klog.Logger) {
	go p.backoffQ.waitUntilAlignedWithOrderingWindow(func() {
		p.flushBackoffQCompleted(logger)
	}, p.stop)
	go wait.Until(func() {
		p.flushUnschedulableEntitiesLeftover(logger)
	}, 30*time.Second, p.stop)
}

// queueingStrategy 表示调度队列应如何将 Pod 从不可调度池中重新入队。
type queueingStrategy int

const (
	// queueSkip 表示调度队列应跳过将该 Pod 重新入 activeQ/backoffQ。
	queueSkip queueingStrategy = iota
	// queueAfterBackoff 表示调度队列应在退避完成后再重新入队该 Pod。
	queueAfterBackoff
	// queueImmediately 表示调度队列应跳过退避，立即将该 Pod 重新入 activeQ。
	queueImmediately
)

// isEventOfInterest 若该事件被某些插件关注则返回 true。
func (p *PriorityQueue) isEventOfInterest(logger klog.Logger, event fwk.ClusterEvent) bool {
	if framework.ClusterEventIsWildCard(event) {
		// 通配事件可移动被任意插件拒绝的 Pod。
		return true
	}

	for _, hintMap := range p.queueingHintMap {
		for eventToMatch := range hintMap {
			if framework.MatchClusterEvents(eventToMatch, event) {
				// 该事件被某些插件关注。
				return true
			}
		}
	}

	logger.V(6).Info("receive an event that isn't interested by any enabled plugins", "event", event)

	return false
}

// isPodGroupMember 判断 Pod 是否属于某个 PodGroup。
func (p *PriorityQueue) isPodGroupMember(pod *v1.Pod) bool {
	if p.isInPlacePodVerticalScalingSchedulerPreemptionEnabled {
		// 延迟调整大小的 Pod 已在已分配的节点上运行，做的是原子的个体资源调整而非成组调度，
		// 因此必须单独入队和评估，不能作为 PodGroup 处理。
		// TODO: 当前实现将在 Beta 阶段重新评估。
		if resource.IsPodResizeDeferred(pod) {
			return false
		}
	}
	return p.isGenericWorkloadEnabled && pod.Spec.SchedulingGroup != nil
}

// isEntityWorthRequeuing 对实体中的所有 Pod 调用 isPodWorthRequeuing，并返回最高优先级的排队策略。
func (p *PriorityQueue) isEntityWorthRequeuing(logger klog.Logger, entity framework.QueuedEntityInfo, event fwk.ClusterEvent, oldObj, newObj interface{}, hintKeys *preQueueingHintPodKeys) queueingStrategy {
	// 对 PodGroup 而言，只要有一个 Pod 值得重新入队，整个组就值得；但应优先采用更高级别的策略。
	bestStrategy := queueSkip
	entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		strategy := p.isPodWorthRequeuing(logger, pInfo, event, oldObj, newObj, hintKeys)
		if strategy > bestStrategy {
			bestStrategy = strategy
		}
		if bestStrategy == queueImmediately {
			return false
		}
		return true
	})
	return bestStrategy
}

// isPodWorthRequeuing 仅调用 pInfo.UnschedulablePlugins 和 pInfo.PendingPlugins 中注册插件的 QueueingHintFn。
//
// 若任意 pInfo.PendingPlugins 返回 Queue，调度队列应跳过 backoffQ，直接将该 Pod 入 activeQ。
// 若任意 pInfo.UnschedulablePlugins 返回 Queue，调度队列应根据该 Pod 剩余退避时间将其入 activeQ/backoffQ。
func (p *PriorityQueue) isPodWorthRequeuing(logger klog.Logger, pInfo *framework.QueuedPodInfo, event fwk.ClusterEvent, oldObj, newObj interface{}, hintKeys *preQueueingHintPodKeys) queueingStrategy {
	rejectorPlugins := pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins)
	if rejectorPlugins.Len() == 0 {
		logger.V(6).Info("Worth requeuing because no failed plugins", "pod", klog.KObj(pInfo.Pod))
		return queueAfterBackoff
	}

	if framework.ClusterEventIsWildCard(event) {
		// 若通配事件的 newObj 中带有 Pod，说明该事件仅针对该 Pod 生效。
		// 具体来说，EventForceActivate 可能会在 newObj 中携带目标 Pod。
		if newObj != nil {
			if pod, ok := newObj.(*v1.Pod); !ok || pod.UID != pInfo.Pod.UID {
				// 该通配事件不是针对此 Pod 的。
				if ok {
					logger.V(6).Info("Not worth requeuing because the event is wildcard, but for another pod", "pod", klog.KObj(pInfo.Pod), "event", event.Label(), "newObj", klog.KObj(pod))
				}
				return queueSkip
			}
		}

		// 这是调用方希望强制所有 Pod 移入 activeQ/backoffQ 的特殊通配事件。
		// 此时返回 queueAfterBackoff，同时清空所有阻塞插件。
		logger.V(6).Info("Worth requeuing because the event is wildcard", "pod", klog.KObj(pInfo.Pod), "event", event.Label())
		return queueAfterBackoff
	}

	hintMap, ok := p.queueingHintMap[pInfo.Pod.Spec.SchedulerName]
	if !ok {
		// 除非有 bug，否则不应走到这里。
		utilruntime.HandleErrorWithLogger(logger, nil, "No QueueingHintMap is registered for this profile", "profile", pInfo.Pod.Spec.SchedulerName, "pod", klog.KObj(pInfo.Pod))
		return queueAfterBackoff
	}

	pod := pInfo.Pod
	podNN := types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}
	queueStrategy := queueSkip
	for eventToMatch, hintfns := range hintMap {
		if !framework.MatchClusterEvents(eventToMatch, event) {
			continue
		}

		for _, hintfn := range hintfns {
			if !rejectorPlugins.Has(hintfn.PluginName) {
				// 非拒绝插件的 hintfn 直接跳过。
				continue
			}
			// 按插件粒度的 PreQueueingHint 收窄：若该 Pod 未被此插件的 PreQueueingHintFn 选中，
			// 则跳过该插件的 QueueingHintFn。
			if hintKeys != nil {
				if pluginPods, ok := hintKeys.perPlugin[hintfn.PluginName]; ok {
					if !pluginPods.Has(podNN) {
						continue
					}
				}
			}

			start := time.Now()
			hint, err := hintfn.QueueingHintFn(logger, pod, oldObj, newObj)
			if err != nil {
				// QueueingHintFn 报错时按 Queue 处理，避免 Pod 一直卡在不可调度池中。
				oldObjMeta, newObjMeta, asErr := util.As[klog.KMetadata](oldObj, newObj)
				if asErr != nil {
					utilruntime.HandleErrorWithLogger(logger, err, "QueueingHintFn returns error", "event", event, "plugin", hintfn.PluginName, "pod", klog.KObj(pod))
				} else {
					utilruntime.HandleErrorWithLogger(logger, err, "QueueingHintFn returns error", "event", event, "plugin", hintfn.PluginName, "pod", klog.KObj(pod), "oldObj", klog.KObj(oldObjMeta), "newObj", klog.KObj(newObjMeta))
				}
				hint = fwk.Queue
			}
			p.metricsRecorder.ObserveQueueingHintDurationAsync(hintfn.PluginName, event.Label(), queueingHintToLabel(hint, err), metrics.SinceInSeconds(start))

			if hint == fwk.QueueSkip {
				continue
			}

			if pInfo.PendingPlugins.Has(hintfn.PluginName) {
				// Pending 插件的 Queue 视为 queueImmediately；它是最高优先级，可直接返回。
				return queueImmediately
			}

			// Unschedulable 插件的 Queue 视为 queueAfterBackoff。

			if pInfo.PendingPlugins.Len() == 0 {
				// 该 Pod 没有 Pending 插件（它们才能产生 queueImmediately），而 queueAfterBackoff 是次高优先级，可直接返回。
				return queueAfterBackoff
			}

			// 该 Pod 注册了 Pending 插件，不能直接返回；
			// 需继续检查它们是否返回 Queue，若是则返回 queueImmediately。
			queueStrategy = queueAfterBackoff
		}
	}

	return queueStrategy
}

// queueingHintToLabel 将 QHint 的提示和错误转换为指标标签字符串。
func queueingHintToLabel(hint fwk.QueueingHint, err error) string {
	if err != nil {
		return metrics.QueueingHintResultError
	}

	switch hint {
	case fwk.Queue:
		return metrics.QueueingHintResultQueue
	case fwk.QueueSkip:
		return metrics.QueueingHintResultQueueSkip
	}

	// 不应走到这里。
	return ""
}

// runPreEnqueuePlugins 遍历每个已注册 PreEnqueuePlugin 的 PreEnqueue 函数，
// 并更新 entity.GatingPlugin 和 entity.UnschedulablePlugins。
// 注意：需要把失败的插件记录到 `entity` 上，这样相关集群事件才能把该实体移回 activeQ。
func (p *PriorityQueue) runPreEnqueuePlugins(ctx context.Context, entity framework.QueuedEntityInfo) {
	var anyGatedPodInfo *framework.QueuedPodInfo
	// 即使首个 Pod 已被 gating 就可以停止，也仍需为每个 Pod 运行插件，以填充各 Pod 的指标。
	entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		p.runPreEnqueuePluginsForPod(ctx, pInfo)
		if pInfo.Gated() {
			// 只要有一个 Pod 被 gating，整个实体就算被 gating；
			// 否则该 Pod 就得拆成单独的实体对象单独跟踪，会让流程变复杂。
			anyGatedPodInfo = pInfo
		}
		return true
	})
	if anyGatedPodInfo != nil {
		// 只从单个 Pod 复制 gating 插件信息即可，因为若实体是 PodGroup，
		// 整个组解除 gating 前，包括该 Pod 在内的所有 Pod 都必须先解除 gating。
		entity.SetGatingPlugin(anyGatedPodInfo.QueueingParams.GatingPlugin, anyGatedPodInfo.QueueingParams.GatingPluginEvents)
	} else {
		entity.SetGatingPlugin("", nil)
	}
}

func (p *PriorityQueue) runPreEnqueuePluginsForPod(ctx context.Context, pInfo *framework.QueuedPodInfo) {
	var s *fwk.Status
	pod := pInfo.Pod
	startTime := p.clock.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(preEnqueue, s.Code().String(), pod.Spec.SchedulerName).Observe(metrics.SinceInSeconds(startTime))
	}()

	shouldRecordMetric := rand.Intn(100) < p.pluginMetricsSamplePercent
	gatingPlugin := pInfo.GatingPlugin
	if gatingPlugin != "" {
		// 优先运行上一次 gating 的插件，它可能仍然拒绝该 Pod。
		s := p.runPreEnqueuePlugin(ctx, p.preEnqueuePluginMap[pod.Spec.SchedulerName][gatingPlugin], pInfo, shouldRecordMetric)
		if !s.IsSuccess() {
			// 已被 gating，无需再遍历其他插件。
			return
		}
	}

	for _, pl := range p.preEnqueuePluginMap[pod.Spec.SchedulerName] {
		if gatingPlugin != "" && pl.Name() == gatingPlugin {
			// 上面已经运行过。
			continue
		}
		s := p.runPreEnqueuePlugin(ctx, pl, pInfo, shouldRecordMetric)
		if !s.IsSuccess() {
			// 已被 gating，无需再遍历其他插件。
			return
		}
	}
	// 所有插件均通过。
	pInfo.GatingPlugin = ""
}

// runPreEnqueuePlugin 运行 PreEnqueue 插件，并按需更新 pInfo 的字段。
func (p *PriorityQueue) runPreEnqueuePlugin(ctx context.Context, pl fwk.PreEnqueuePlugin, pInfo *framework.QueuedPodInfo, shouldRecordMetric bool) *fwk.Status {
	pod := pInfo.Pod
	startTime := p.clock.Now()
	s := pl.PreEnqueue(ctx, pod)
	if shouldRecordMetric {
		p.metricsRecorder.ObservePluginDurationAsync(preEnqueue, pl.Name(), s.Code().String(), p.clock.Since(startTime).Seconds())
	}
	if s.IsSuccess() {
		// 无需修改 GatingPlugin：若后续插件 gating 该 Pod 会被覆盖，全部通过时也会被清空。
		return s
	}
	// 该插件的指标未增加过时再增加并记录，避免重复计数。
	if !pInfo.UnschedulablePlugins.Has(pl.Name()) && !pInfo.PendingPlugins.Has(pl.Name()) {
		metrics.UnschedulableReason(pl.Name(), pod.Spec.SchedulerName).Inc()
	}
	pInfo.UnschedulablePlugins.Insert(pl.Name())
	pInfo.GatingPlugin = pl.Name()
	pInfo.GatingPluginEvents = p.pluginToEventsMap[pInfo.GatingPlugin]
	if s.Code() == fwk.Error {
		utilruntime.HandleErrorWithContext(ctx, s.AsError(), "Unexpected error running PreEnqueue plugin", "pod", klog.KObj(pod), "plugin", pl.Name())
	} else {
		klog.FromContext(ctx).V(4).Info("Status after running PreEnqueue plugin", "pod", klog.KObj(pod), "plugin", pl.Name(), "status", s)
	}

	return s
}

// AddNominatedPod 将给定的 Pod 加入 nominator。
// 它会锁住 PriorityQueue，避免与其他方法发生竞态。
func (p *PriorityQueue) AddNominatedPod(logger klog.Logger, pi fwk.PodInfo, nominatingInfo *fwk.NominatingInfo) {
	p.lock.Lock()
	p.nominator.addNominatedPod(logger, pi, nominatingInfo)
	p.lock.Unlock()
}

// moveToActiveQ 尝试将实体加入 activeQ。
// 若实体未通过 PreEnqueue 插件，则改为加入 unschedulableEntities。
// 若实体是直接从 backoffQ 移来的，movesFromBackoffQ 应为 true，以便跳过 PreEnqueue 调用。
// strategy 是实体此前的排队策略。strategy 为 nil 表示实体是新增到调度队列的，
// 此时无条件增加入队实体指标；否则根据 strategy 判断实体是否真的跨队列移动，避免指标重复计数。
// 返回值表示实体是否成功入队。
// 调用 moveToActiveQ 前必须先将实体从 backoffQ 中删除。
// 注意：本方法不会唤醒 Pop()，调用方需在调用后执行 activeQ.broadcast()。
func (p *PriorityQueue) moveToActiveQ(logger klog.Logger, entity framework.QueuedEntityInfo, event string, movesFromBackoffQ bool, strategy *queueingStrategy) bool {
	gatedBefore := entity.Gated()
	// SchedulerPopFromBackoffQ 启用时，实体入 backoffQ 时已运行过 PreEnqueue 插件，
	// 从 backoffQ 直接移入 activeQ 无需重复执行。
	skipPreEnqueue := p.isPopFromBackoffQEnabled && movesFromBackoffQ
	if !skipPreEnqueue {
		p.runPreEnqueuePlugins(context.Background(), entity)
	}

	if entity.Gated() {
		if p.unschedulableEntities.get(entity) == nil {
			logger.V(5).Info("Entity moved to an internal scheduling queue, because it is gated", "type", entity.Type(), "entity", klog.KObj(entity), "event", event, "queue", unschedulableQ)
		}
		// 该标志通常在调度失败时清除；但被 flush 的 Pod 若被 gating 就不会尝试调度，
		// 因此在这里清除，确保下次被非 flush 事件唤醒时不带该标志。
		entity.SetWasFlushedFromUnschedulable(false)
		p.unschedulableEntities.addOrUpdate(entity, gatedBefore, event, strategy)
		// 实体未进入 activeQ。
		return false
	}
	now := p.clock.Now()
	entity.SetInitialAttemptTimestamp(now)
	p.unschedulableEntities.delete(entity, gatedBefore)

	p.activeQ.add(logger, entity, event, strategy)
	// 实体成功进入 activeQ。
	return true
}

// moveToBackoffQ 尝试将实体加入退避队列。
// 若 SchedulerPopFromBackoffQ feature gate 启用且实体未通过 PreEnqueue 插件，则改为加入 unschedulableEntities。
// strategy 是实体此前的排队策略。strategy 为 nil 表示实体是新增到调度队列的，
// 此时无条件增加入队实体指标；否则根据 strategy 判断实体是否真的跨队列移动，避免指标重复计数。
// 返回值表示实体是否成功入队。
func (p *PriorityQueue) moveToBackoffQ(logger klog.Logger, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy) bool {
	gatedBefore := entity.Gated()
	// SchedulerPopFromBackoffQ 启用时，PreEnqueue 插件在入 backoffQ 时执行，
	// 弹出时不再重复调用。
	if p.isPopFromBackoffQEnabled {
		p.runPreEnqueuePlugins(context.Background(), entity)
		if entity.Gated() {
			if uInfo := p.unschedulableEntities.get(entity); uInfo == nil {
				logger.V(5).Info("Entity moved to an internal scheduling queue, because it is gated", "type", entity.Type(), "entity", klog.KObj(entity), "event", event, "queue", unschedulableQ)
			}
			// 该标志通常在调度失败时清除；但被 flush 的 Pod 若被 gating 就不会尝试调度，
			// 因此在这里清除，确保下次被非 flush 事件唤醒时不带该标志。
			entity.SetWasFlushedFromUnschedulable(false)
			p.unschedulableEntities.addOrUpdate(entity, gatedBefore, event, strategy)
			return false
		}
	}
	p.unschedulableEntities.delete(entity, gatedBefore)

	p.backoffQ.add(logger, entity, event, strategy)
	return true
}

// Add 将新建的 Pod 加入 activeQ。
// 仅在新增 Pod 时调用，此时它不可能已存在于 active/unschedulable/backoff 队列中。
func (p *PriorityQueue) Add(ctx context.Context, pod *v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.addPod(ctx, pod)
}

// addPod 将 Pod 加入 activeQ；若它是 PodGroup 成员，则加入其所属的已入队 PodGroup。
func (p *PriorityQueue) addPod(ctx context.Context, pod *v1.Pod) {
	logger := klog.FromContext(ctx)
	pInfo := p.newQueuedPodInfo(ctx, pod)
	// 无条件调用 addNominatedPod，确保新增 Pod 的提名信息被记录（即使它被 gating 而未进入 activeQ）。
	// 且必须在 moveToActiveQ 之前调用：否则调度循环可能在提名记录前就弹出并处理该 Pod，造成数据竞态。
	p.nominator.addNominatedPod(logger, pInfo.PodInfo, nil)
	if p.isPodGroupMember(pod) {
		p.addPodGroupMember(logger, pInfo)
		return
	}
	if added := p.moveToActiveQ(logger, pInfo, framework.EventUnscheduledPodAdd.Label(), false, nil); added {
		p.activeQ.broadcast()
	}
}

// addPodGroupMember 将 pInfo 作为其所属 PodGroup 的成员加入调度队列。
func (p *PriorityQueue) addPodGroupMember(logger klog.Logger, pInfo *framework.QueuedPodInfo) {
	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForPod(pInfo.Pod)
	if !hasRoot {
		// 找不到 PodGroup 对象就无法调度该 Pod，
		// 放入 incompletePodGroupPods 等待 PodGroup 被观察到。
		p.incompletePodGroupPods.add(pInfo)
		logger.V(5).Info("Pod with pod group added to incompletePodGroupPods, waiting for one or more parent pod group objects to be observed", "podGroup", pInfo.Pod.Spec.SchedulingGroup.PodGroupName, "pod", klog.KObj(pInfo))
		return
	}

	if added := p.addToPodGroupIfExists(logger, pInfo, rootInfoLookup); added {
		return
	}

	if p.activeQ.isLastPoppedEntity(rootInfoLookup) {
		// 若刚弹出的正是对应的 PodGroup，就把 Pod 放入 pendingPodGroupPods，
		// 待该 PodGroup 重新入队时再合并进去。
		p.pendingPodGroupPods.add(pInfo)
		logger.V(5).Info("Pod added to pending pod group pods, waiting for its root group to be requeued", "rootType", rootInfoLookup.Type(), "root", klog.KObj(rootInfoLookup), "pod", klog.KObj(pInfo))
		return
	}

	// 队列中没有该组，当前 Pod 是首个成员，需新建一个组实体。
	rootInfo := p.newQueuedPodGroupInfoFromRootLookup(logger, pInfo, rootInfoLookup)
	if added := p.moveToActiveQ(logger, rootInfo, framework.EventUnscheduledPodAdd.Label(), false, nil); added {
		p.activeQ.broadcast()
	}
	logger.V(5).Info("Pod was added to root group as the first member pod", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "pod", klog.KObj(pInfo))
}

// deleteFromAnyQueue 从实体可能所在的任意队列中删除该实体，
// 并返回实体本身及其所在队列对应的排队策略。
// 若未找到该实体，则返回 nil。
func (p *PriorityQueue) deleteFromAnyQueue(entityLookup framework.QueuedEntityInfo) (framework.QueuedEntityInfo, queueingStrategy) {
	if entity := p.activeQ.delete(entityLookup); entity != nil {
		return entity, queueImmediately
	}
	if entity := p.backoffQ.delete(entityLookup); entity != nil {
		return entity, queueAfterBackoff
	}
	if entity := p.unschedulableEntities.get(entityLookup); entity != nil {
		p.unschedulableEntities.delete(entity, entity.Gated())
		return entity, queueSkip
	}
	return nil, queueSkip
}

// addToPodGroupIfExists 尝试将 pInfo 作为其所属 PodGroup 的成员加入调度队列中已有的组。
// 若成功加入已有 PodGroup，则返回 true。
func (p *PriorityQueue) addToPodGroupIfExists(logger klog.Logger, pInfo *framework.QueuedPodInfo, rootInfoLookup framework.QueuedEntityInfo) bool {
	entity, strategy := p.deleteFromAnyQueue(rootInfoLookup)
	if entity == nil {
		return false
	}
	rootInfo := entity.(*framework.QueuedPodGroupInfo)
	rootInfo.AddPod(pInfo)
	queue := p.requeueEntityWithQueueingStrategy(logger, rootInfo, strategy, framework.EventUnscheduledPodAdd.Label())
	if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
		p.activeQ.broadcast()
	}
	logger.V(5).Info("Pod added to existing root group info", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "pod", klog.KObj(pInfo), "queue", queue)
	return true
}

// Activate 将给定的 Pod 移入 activeQ。
// 若 Pod 不在 unschedulableEntities 或 backoffQ 中且处于 in-flight 状态，
// 则注册通配事件，待其返回队列时重新入队。
// 若 Pod 不在上述两处且不处于 in-flight 状态（即完全未知的 Pod），Activate 会忽略它。
// 若激活的 Pod 属于某个 PodGroup，则激活整个根 PodGroup 或组合 PodGroup。
func (p *PriorityQueue) Activate(logger klog.Logger, pods map[string]*v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	activated := false
	for _, pod := range pods {
		var entityLookup framework.QueuedEntityInfo
		if p.isPodGroupMember(pod) {
			var hasRoot bool
			entityLookup, hasRoot = p.workloadForest.getRootLookupInfoForPod(pod)
			if !hasRoot {
				continue
			}
		} else {
			entityLookup = newQueuedPodInfoForLookup(pod)
		}
		if p.activate(logger, entityLookup) {
			activated = true
			continue
		}

		// 若 Pod 处于 in-flight 状态，注册激活事件，待其返回时重新入队。
		// 对 in-tree 插件而言，这覆盖了抢占插件的场景：异步抢占 API 调用在 Pod 返回队列前已完成或已失败。
		p.activeQ.addEventsIfPodInFlight(nil, pod, []fwk.ClusterEvent{framework.EventForceActivate})
	}

	if activated {
		p.activeQ.broadcast()
	}
}

func (p *PriorityQueue) activate(logger klog.Logger, entityLookup framework.QueuedEntityInfo) bool {
	var entity framework.QueuedEntityInfo
	var movesFromBackoffQ bool
	// 检查实体是否在 unschedulableEntities 或 backoffQ 中。
	if entity = p.unschedulableEntities.get(entityLookup); entity == nil {
		// 实体可能在 backoffQ 中。这里直接尝试删除，
		// 避免它刚被移入 activeQ 前又从 backoffQ 弹出。
		if entity = p.backoffQ.delete(entityLookup); entity == nil {
			// backoffQ 中也没有，不激活。
			return false
		}
		movesFromBackoffQ = true
	}

	if entity == nil {
		// 冗余的安全检查，不应走到这里。
		utilruntime.HandleErrorWithLogger(logger, nil, "Internal error: cannot obtain entity")
		return false
	}

	return p.moveToActiveQ(logger, entity, framework.ForceActivate, movesFromBackoffQ, nil)
}

// SchedulingCycle 返回当前调度周期号。
func (p *PriorityQueue) SchedulingCycle() int64 {
	return p.activeQ.schedulingCycle()
}

// determineSchedulingHintForInFlightPod 根据 Pod 的不可调度插件，
// 并结合 in-flight 期间发生的事件，确定该 Pod 的排队提示。
func (p *PriorityQueue) determineSchedulingHintForInFlightPod(logger klog.Logger, pInfo *framework.QueuedPodInfo) queueingStrategy {
	if len(pInfo.UnschedulablePlugins) == 0 && len(pInfo.PendingPlugins) == 0 {
		// 该 Pod 没有关联的失败插件，说明是 kube-apiserver 临时故障等异常情况导致其返回队列。
		// 此时应重试调度，否则它可能要等到下次 flush 才会被处理。
		return queueAfterBackoff
	}

	events, err := p.activeQ.clusterEventsForPod(logger, pInfo)
	if err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Error getting cluster events for pod", "pod", klog.KObj(pInfo.Pod))
		return queueAfterBackoff
	}

	// 检查是否存在能让该 Pod 重新变为可调度的事件。
	queueingStrategy := queueSkip
	for _, e := range events {
		logger.V(5).Info("Checking event for in-flight pod", "pod", klog.KObj(pInfo.Pod), "event", e.event.Label())

		switch p.isPodWorthRequeuing(logger, pInfo, e.event, e.oldObj, e.newObj, nil) {
		case queueSkip:
			continue
		case queueImmediately:
			// queueImmediately 优先级最高，无需继续检查其余事件。
			return queueImmediately
		case queueAfterBackoff:
			// 记录 queueAfterBackoff 提示。
			queueingStrategy = queueAfterBackoff
			if pInfo.PendingPlugins.Len() == 0 {
				// 该 Pod 没有 Pending 插件（它们才能产生 queueImmediately），而 queueAfterBackoff 是次高优先级，可直接返回。
				return queueAfterBackoff
			}
		}
	}
	return queueingStrategy
}

// AddUnschedulablePodIfNotPresent 将不可调度的 Pod 插回队列，若它已在队列中则跳过。
func (p *PriorityQueue) AddUnschedulablePodIfNotPresent(logger klog.Logger, pInfo *framework.QueuedPodInfo, podSchedulingCycle int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// 无论如何该 Pod 都会回到队列，必须调用 Done 解除 in-flight 标记。
	calledDone := false
	defer func() {
		if !calledDone {
			p.Done(pInfo.Pod.UID)
		}
	}()

	pod := pInfo.Pod
	if p.unschedulableEntities.get(pInfo) != nil {
		return fmt.Errorf("Pod %v is already present in unschedulable queue", klog.KObj(pod))
	}

	if p.activeQ.has(pInfo) {
		return fmt.Errorf("Pod %v is already present in the active queue", klog.KObj(pod))
	}
	if p.backoffQ.has(pInfo) {
		return fmt.Errorf("Pod %v is already present in the backoff queue", klog.KObj(pod))
	}

	if len(pInfo.UnschedulablePlugins) == 0 && len(pInfo.PendingPlugins) == 0 {
		// 没有插件拒绝，说明是网络异常等意外错误导致 Pod 返回。
		pInfo.ConsecutiveErrorsCount++
	} else {
		// 是被插件拒绝而非意外错误导致返回。
		pInfo.UnschedulableCount++
		// 错误已消失，重置连续错误计数。
		pInfo.ConsecutiveErrorsCount = 0
	}
	// Pod 重新入队，刷新时间戳。
	pInfo.Timestamp = p.clock.Now()
	// 计数和时间戳都变了，缓存的退避到期时间已失效，需要清除。
	pInfo.BackoffExpiration = time.Time{}
	// Pod 是经过一次调度尝试后返回队列的，清除 flush 标志。
	pInfo.WasFlushedFromUnschedulable = false
	pInfo.FlushTimestamp = time.Time{}

	if p.isPodGroupMember(pod) {
		// 必须在加入不可调度的 PodGroup 成员前调用 Done。
		// 否则 Pod 加入 active PodGroup 后，Pop 可能看到它仍处于未 Done 状态，产生竞态。
		p.Done(pInfo.Pod.UID)
		calledDone = true
		// Pod 属于 PodGroup 时，按组的上下文处理。
		p.addUnschedulablePodGroupMember(logger, pInfo)
		return nil
	}

	p.activeQ.clearPoppedEntity()
	rejectorPlugins := pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins)
	for plugin := range rejectorPlugins {
		metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Inc()
	}

	// 检查调度期间发生的事件是否可能改变该 Pod 的调度结果。
	schedulingHint := p.determineSchedulingHintForInFlightPod(logger, pInfo)

	// 根据上述提示尝试将该 Pod 重新入 activeQ/backoffQ。
	queue := p.requeueEntityWithQueueingStrategy(logger, pInfo, schedulingHint, framework.ScheduleAttemptFailure)
	logger.V(3).Info("Pod moved to an internal scheduling queue", "pod", klog.KObj(pod), "event", framework.ScheduleAttemptFailure, "queue", queue, "schedulingCycle", podSchedulingCycle, "hint", schedulingHint, "unschedulable plugins", rejectorPlugins)
	if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
		// Pod 进入 activeQ（或可弹出的 backoffQ）后需唤醒 Pop。
		p.activeQ.broadcast()
	}

	return nil
}

// addUnschedulablePodGroupMember 将 pInfo 作为其所属 PodGroup 的成员加入调度队列。
func (p *PriorityQueue) addUnschedulablePodGroupMember(logger klog.Logger, pInfo *framework.QueuedPodInfo) {
	rejectorPlugins := pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins)
	for plugin := range rejectorPlugins {
		metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Inc()
	}

	// Pod 只需重新加入调度队列即可：已入队的 PodGroup 可能处于任意状态，
	// addPodGroupMember 会处理所有情况。
	p.addPodGroupMember(logger, pInfo)
}

// AddAttemptedPodGroupIfNeeded 将已尝试调度的 PodGroup 加回调度队列。
// 若没有待处理 Pod，则不会把该 PodGroup 加回队列。
// 应与 PodGroup 调度周期同步调用。
// 若 PodGroup 调度成功，其余 Pod 将直接重新入 activeQ，而不是走退避。
// 若仍有不可调度的 Pod，PodGroup 会保留其时间戳，以便在当前周期后立即触发抢占，
// 除非期间来了更高优先级的 Pod 或 PodGroup。
func (p *PriorityQueue) AddAttemptedPodGroupIfNeeded(logger klog.Logger, pgInfo *framework.QueuedPodGroupInfo, schedulingCycle int64, status *fwk.Status) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.unschedulableEntities.get(pgInfo) != nil {
		return fmt.Errorf("pod group %v is already present in unschedulable queue", klog.KObj(pgInfo))
	}

	if p.activeQ.has(pgInfo) {
		return fmt.Errorf("pod group %v is already present in the active queue", klog.KObj(pgInfo))
	}
	if p.backoffQ.has(pgInfo) {
		return fmt.Errorf("pod group %v is already present in the backoff queue", klog.KObj(pgInfo))
	}

	p.activeQ.clearPoppedEntity()
	// 取出所有 pending Pod，准备放回 PodGroup。
	var pendingPods []*framework.QueuedPodInfo
	for _, leafPodGroup := range p.workloadForest.getLeafPodGroups(logger, pgInfo) {
		pods := p.pendingPodGroupPods.clear(leafPodGroup.Namespace, leafPodGroup.Name)
		pendingPods = append(pendingPods, pods...)
	}
	if len(pendingPods) == 0 {
		// 没有 pending Pod，无需重入队。
		return nil
	}
	// 本调度周期中是否至少有一个 Pod 不可调度或出错。
	hadUnschedulableOrErrorPods := hasIntersection(pgInfo.QueuedPodInfos, pendingPods)

	rootInfo := p.workloadForest.buildQueuedPodGroupInfo(logger, pgInfo)
	if rootInfo == nil {
		// 不应发生：若 PodGroup 被删除，其 Pod 会先进入 incompletePodGroupPods，
		// 此时 pendingPods 应为空。
		for _, pInfo := range pendingPods {
			p.incompletePodGroupPods.add(pInfo)
		}
		logger.Error(nil, "Root group no longer exists on requeue attempt, decomposed and moved pods to incompletePodGroupPods", "rootType", pgInfo.Type(), "root", klog.KObj(pgInfo))
		return nil
	}

	for _, pInfo := range pendingPods {
		rootInfo.AddPod(pInfo)
	}
	rootInfo.QueueingParams = pgInfo.QueueingParams
	pgInfo = rootInfo

	requeueWithoutBackoff := false
	preserveTimestamp := false

	if status.IsSuccess() {
		// PodGroup 调度成功，其余 Pod 直接重新入 activeQ，不走退避。
		requeueWithoutBackoff = true
		// 若仍有不可调度或出错的 Pod，保留时间戳，让下一次调度尝试立即为 PodGroup 触发抢占。
		preserveTimestamp = hadUnschedulableOrErrorPods
		// 调度已成功，重置错误与不可调度计数。
		pgInfo.ConsecutiveErrorsCount = 0
		pgInfo.UnschedulableCount = 0
	} else if status.IsRejected() {
		// 是被插件拒绝，而非网络等意外错误导致返回。
		pgInfo.UnschedulableCount++
		// 错误已消失，重置连续错误计数。
		pgInfo.ConsecutiveErrorsCount = 0
		// 计数和时间戳变了，缓存的退避到期时间已失效。
		pgInfo.BackoffExpiration = time.Time{}
	} else {
		// 是网络异常等意外错误导致返回。
		pgInfo.ConsecutiveErrorsCount++
		// 计数和时间戳变了，缓存的退避到期时间已失效。
		pgInfo.BackoffExpiration = time.Time{}
	}

	if !preserveTimestamp {
		pgInfo.Timestamp = p.clock.Now()
	}

	// PodGroup 经过一次调度尝试后返回队列，清除 flush 标志。
	pgInfo.WasFlushedFromUnschedulable = false
	pgInfo.FlushTimestamp = time.Time{}

	rejectorPlugins := pgInfo.UnschedulablePlugins.Union(pgInfo.PendingPlugins)

	// 尝试将 PodGroup 重新入 activeQ 或 backoffQ。
	// 若 PreEnqueue 未通过，则移入 unschedulableEntities。
	queue := unschedulableQ
	if !requeueWithoutBackoff && p.backoffQ.isEntityBackingoff(pgInfo) {
		if added := p.moveToBackoffQ(logger, pgInfo, framework.ScheduleAttemptFailure, nil); added {
			queue = backoffQ
		}
	} else {
		if added := p.moveToActiveQ(logger, pgInfo, framework.ScheduleAttemptFailure, false, nil); added {
			queue = activeQ
		}
	}
	logger.V(3).Info("Pod group moved to an internal scheduling queue", "podGroup", klog.KObj(pgInfo), "event", framework.ScheduleAttemptFailure, "queue", queue, "schedulingCycle", schedulingCycle, "unschedulable plugins", rejectorPlugins)
	if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
		// PodGroup 进入 activeQ（或可弹出的 backoffQ）后需唤醒 Pop。
		p.activeQ.broadcast()
	}

	return nil
}

// hasIntersection 若 slice 中有任意 Pod 出现在 map 中则返回 true。
func hasIntersection(m map[fwk.EntityKey][]*framework.QueuedPodInfo, slice []*framework.QueuedPodInfo) bool {
	uids := sets.New[types.UID]()
	for _, pInfos := range m {
		for _, pInfo := range pInfos {
			uids.Insert(pInfo.Pod.UID)
		}
	}

	for _, pInfo := range slice {
		if uids.Has(pInfo.Pod.UID) {
			return true
		}
	}
	return false
}

// flushBackoffQCompleted 将 backoffQ 中已完成退避的所有 Pod 移入 activeQ。
func (p *PriorityQueue) flushBackoffQCompleted(logger klog.Logger) {
	p.lock.Lock()
	defer p.lock.Unlock()
	activated := false
	podsCompletedBackoff := p.backoffQ.popAllBackoffCompleted(logger)
	for _, pInfo := range podsCompletedBackoff {
		if added := p.moveToActiveQ(logger, pInfo, framework.BackoffComplete, true, nil); added {
			activated = true
		}
	}
	if activated {
		p.activeQ.broadcast()
	}
}

// flushUnschedulableEntitiesLeftover 将停留在 unschedulableEntities 中超过
// podMaxInUnschedulablePodsDuration 的实体移入 backoffQ 或 activeQ。
func (p *PriorityQueue) flushUnschedulableEntitiesLeftover(logger klog.Logger) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var entitiesToMove []framework.QueuedEntityInfo
	currentTime := p.clock.Now()
	for _, entity := range p.unschedulableEntities.entityInfoMap {
		lastScheduleTime := entity.GetTimestamp()
		if flushTime := entity.GetFlushTimestamp(); flushTime.After(lastScheduleTime) {
			lastScheduleTime = flushTime
		}
		// TODO(macsko): 待 PodGroup 支持 queueing hints 后，可能需要为其设置更长的最大停留时间。
		if currentTime.Sub(lastScheduleTime) > p.podMaxInUnschedulablePodsDuration {
			entity.SetWasFlushedFromUnschedulable(true)
			entity.SetFlushTimestamp(currentTime)
			entitiesToMove = append(entitiesToMove, entity)
		}
	}

	if len(entitiesToMove) > 0 {
		p.moveEntitiesToActiveOrBackoffQueue(logger, entitiesToMove, framework.EventUnschedulableTimeout, nil, nil, nil)
	}
}

// Pop 移除并返回 activeQ 的队首实体。activeQ 为空时阻塞，直到有新实体入队。
// 每当弹出一个 Pod 时，调度周期号递增。
// 注意：本方法任何时刻都不应持有 p.lock，否则会降低调度吞吐量。
func (p *PriorityQueue) Pop(logger klog.Logger) (framework.QueuedEntityInfo, error) {
	return p.activeQ.pop(logger)
}

// Done 必须对 Pop 返回的 Pod 调用，以便队列跟踪当前正在处理的 Pod。
func (p *PriorityQueue) Done(pod types.UID) {
	p.activeQ.done(pod)
}

// InFlightPods 返回当前处于 in-flight 状态的 Pod。
func (p *PriorityQueue) InFlightPods() []*v1.Pod {
	return p.activeQ.listInFlightPods()
}

// Update 更新 activeQ 或 backoffQ 中的 Pod（若存在）。
// 否则，若 Pod 的更新可能使其变得可调度，则将其从不可调度队列移除并加入 activeQ。
// 若 Pod 不在任何队列中，则将其加入 activeQ。
func (p *PriorityQueue) Update(ctx context.Context, oldPod, newPod *v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()
	logger := klog.FromContext(ctx)

	// 这里被更新的是 Pod 本身，因此事件关联的资源是 TargetPod。
	events := framework.PodSchedulingPropertiesChange(newPod, oldPod, true)

	var entityLookup framework.QueuedEntityInfo
	if p.isPodGroupMember(oldPod) {
		var hasRoot bool
		entityLookup, hasRoot = p.workloadForest.getRootLookupInfoForPod(oldPod)
		if !hasRoot {
			if pInfo := p.incompletePodGroupPods.update(newPod); pInfo != nil {
				p.UpdateNominatedPod(logger, oldPod, pInfo.PodInfo)
				pInfo.PodSignature = p.signPod(ctx, newPod)
			}
			return
		}
	} else {
		entityLookup = newQueuedPodInfoForLookup(oldPod)
	}

	updated := false
	// 以下代码在 activeQ 锁内执行，确保期间实体不会从 activeQ 或 backoffQ 被弹出，
	// 从而保证事件注册与实体更新是一致的。
	// 只锁 Update 的这一段即可：其余部分实体在 unschedulableEntities 中，已受 p.lock 保护。
	p.activeQ.underLock(func(unlockedActiveQ unlockedActiveQueuer) {
		// in-flight Pod 回队时会使用 informer 缓存中的最新版本，这与事件携带的内容一致。
		// 记录这次 Pod 更新，因为若它被拒绝后返回队列，该更新可能使其变为可调度。
		// 待 updatePodInSchedulingQueue 改为调用 MoveAllToActiveOrBackoffQueue 后可清理此处逻辑。
		// 背景见 https://github.com/kubernetes/kubernetes/pull/125578#discussion_r1648338033 。
		if exists := unlockedActiveQ.addEventsIfPodInFlight(oldPod, newPod, events); exists {
			logger.V(6).Info("The pod doesn't need to be queued for now because it's being scheduled and will be queued back if necessary", "pod", klog.KObj(newPod))
			updated = true
			return
		}
		// 实体已在 activeQ 中，直接原地更新 Pod。
		if pInfo := unlockedActiveQ.update(newPod, entityLookup); pInfo != nil {
			p.UpdateNominatedPod(logger, oldPod, pInfo.PodInfo)
			pInfo.PodSignature = p.signPod(ctx, newPod)
			updated = true
			return
		}

		// 实体在 backoffQ 中，直接原地更新 Pod。
		if pInfo := p.backoffQ.update(newPod, entityLookup); pInfo != nil {
			p.UpdateNominatedPod(logger, oldPod, pInfo.PodInfo)
			pInfo.PodSignature = p.signPod(ctx, newPod)
			updated = true
			return
		}
	})

	if updated {
		return
	}

	// Pod 在不可调度队列中，更新可能使其变为可调度。
	if entity := p.unschedulableEntities.get(entityLookup); entity != nil {
		pInfo, err := entity.Update(newPod)
		if err != nil {
			logger.Error(err, "Failed to update pod in an entity", "type", entity.Type(), "entity", klog.KObj(entity), "pod", klog.KObj(newPod))
			// 优雅降级：实体更新失败时直接把 newPod 加入队列。
			p.addPod(ctx, newPod)
			return
		}
		p.UpdateNominatedPod(logger, oldPod, pInfo.PodInfo)
		pInfo.PodSignature = p.signPod(ctx, newPod)

		// 不可调度 Pod 更新时，通过 QueueingHint 判断更新是否让实体变为可调度。
		// 若插件的拒绝可通过 Pod 自身更新解除，它就必须实现 Pod/Update 事件的 QueueingHint。
		for _, evt := range events {
			// 这里先捕获 entityRef 用于日志，避免后续日志中的数据竞态：
			// requeueEntityWithQueueingStrategy 重新入 activeQ 的 Pod 可能很快被弹出处理，
			// 其失败处理逻辑会覆写 PodInfo 中的 Pod 对象，而日志正要访问它。
			entityRef := klog.KObj(entity)
			hint := p.isEntityWorthRequeuing(logger, entity, evt, oldPod, newPod, nil)
			queue := p.requeueEntityWithQueueingStrategy(logger, entity, hint, evt.Label())
			if queue != unschedulableQ {
				logger.V(5).Info("Entity moved to an internal scheduling queue because the Pod is updated", "type", entity.Type(), "entity", entityRef, "pod", klog.KObj(newPod), "event", evt.Label(), "queue", queue)
			}
			if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
				p.activeQ.broadcast()
				break
			}
		}
		return
	}

	if p.isPodGroupMember(newPod) {
		if pInfo := p.pendingPodGroupPods.update(newPod); pInfo != nil {
			p.UpdateNominatedPod(logger, oldPod, pInfo.PodInfo)
			pInfo.PodSignature = p.signPod(ctx, newPod)
			return
		}
	}
	// 实体不在任何队列中，直接加入。
	p.addPod(ctx, newPod)
}

// decreaseUnschedulableReasonMetric 减少 UnschedulablePlugins 和 PendingPlugins 中拒绝插件的指标。
func decreaseUnschedulableReasonMetric(entity framework.QueuedEntityInfo) {
	entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		for plugin := range pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins) {
			metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Dec()
		}
		return true
	})
}

// Delete 从 activeQ、backoffQ 或 unschedulableEntities 中删除实体。
// 假定 Pod 只存在于其中一处。
func (p *PriorityQueue) Delete(logger klog.Logger, pod *v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.DeleteNominatedPodIfExists(pod)
	if p.isPodGroupMember(pod) {
		p.deletePodGroupMember(logger, pod)
	} else {
		p.deletePod(pod)
	}
}

// deletePodGroupMember 从队列中的 PodGroup 移除一个 Pod。
// 若移除后 PodGroup 为空，则将整个 PodGroup 从队列移除。
func (p *PriorityQueue) deletePodGroupMember(logger klog.Logger, pod *v1.Pod) {
	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForPod(pod)
	if !hasRoot {
		pInfo := p.incompletePodGroupPods.delete(pod)
		if pInfo == nil {
			return
		}
		// 删除 Pod 时同步扣减相关指标。
		for plugin := range pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins) {
			metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Dec()
		}
		logger.V(5).Info("Pod deleted from incompletePodGroupPods", "pod", klog.KObj(pInfo), "podGroup", pInfo.Pod.Spec.SchedulingGroup.PodGroupName)
		return
	}

	entity, strategy := p.deleteFromAnyQueue(rootInfoLookup)
	if entity == nil {
		pInfo := p.pendingPodGroupPods.delete(pod)
		if pInfo == nil {
			return
		}

		logger.V(5).Info("Pod belonging to a root group got deleted from pending pod group info", "rootType", rootInfoLookup.Type(), "root", klog.KObj(rootInfoLookup), "pod", klog.KObj(pInfo))
		// 删除 Pod 时同步扣减相关指标。
		for plugin := range pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins) {
			metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Dec()
		}
		return
	}
	pgInfo := entity.(*framework.QueuedPodGroupInfo)

	pInfo := pgInfo.RemovePod(pod)
	if pInfo != nil {
		// 删除 Pod 时同步扣减相关指标。
		for plugin := range pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins) {
			metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Dec()
		}
		logger.V(5).Info("Pod deleted from existing root group info", "rootType", pgInfo.Type(), "root", klog.KObj(pgInfo), "pod", klog.KObj(pInfo))
	}
	if !pgInfo.HasQueuedPodInfos() {
		// PodGroup 已空，无需加回任何队列。
		return
	}

	queue := p.requeueEntityWithQueueingStrategy(logger, pgInfo, strategy, framework.EventUnscheduledPodAdd.Label())
	if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
		p.activeQ.broadcast()
	}
}

// deletePod 从队列中删除单个 Pod。
func (p *PriorityQueue) deletePod(pod *v1.Pod) {
	pInfoLookup := newQueuedPodInfoForLookup(pod)

	// 依次在各队列中查找并删除 Pod。
	if entity := p.activeQ.delete(pInfoLookup); entity != nil {
		// 删除 Pod 时同步扣减相关指标。
		decreaseUnschedulableReasonMetric(entity)
		return
	}

	if entity := p.backoffQ.delete(pInfoLookup); entity != nil {
		// 删除 Pod 时同步扣减相关指标。
		decreaseUnschedulableReasonMetric(entity)
		return
	}

	if entity := p.unschedulableEntities.get(pInfoLookup); entity != nil {
		p.unschedulableEntities.delete(pInfoLookup, entity.Gated())
		// 删除 Pod 时同步扣减相关指标。
		decreaseUnschedulableReasonMetric(entity)
	}
}

// AddPodGroup 向队列新增一个 PodGroup 对象，
// 并将与该 PodGroup 关联的所有 Pod 重新入队。
func (p *PriorityQueue) AddPodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.workloadForest.addPodGroup(podGroup)

	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForPodGroup(podGroup)
	if !hasRoot {
		// 根节点不存在，Pod 应继续留在 incompletePodGroupPods 等待。
		return
	}

	p.activeQ.underLock(func(unlockedActiveQ unlockedActiveQueuer) {
		entity := p.getEntityFromAnyQueue(unlockedActiveQ, rootInfoLookup)
		if entity == nil {
			return
		}

		rootInfo := entity.(*framework.QueuedPodGroupInfo)
		rootInfo.AddPodGroup(podGroup)
		logger.V(5).Info("Pod Group added to the existing root group", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "podGroup", klog.KObj(podGroup))
	})

	pgs := p.workloadForest.getLeafPodGroups(logger, rootInfoLookup)
	var pInfos []*framework.QueuedPodInfo
	for _, pg := range pgs {
		pInfos = append(pInfos, p.incompletePodGroupPods.clear(pg.Namespace, pg.Name)...)
	}

	for _, pInfo := range pInfos {
		// 统一走 addPodGroupMember 处理，可靠覆盖所有情况。
		p.addPodGroupMember(logger, pInfo)
	}
	logger.V(5).Info("Pod Group was added to the root group and its pods were attempted to be moved from incompletePodGroupPods to the queue", "rootType", rootInfoLookup.Type(), "root", klog.KObj(rootInfoLookup), "podGroup", klog.KObj(podGroup), "podsLen", len(pInfos))
}

// UpdatePodGroup 更新队列中已有的 PodGroup 对象。
func (p *PriorityQueue) UpdatePodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.workloadForest.updatePodGroup(podGroup)
	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForPodGroup(podGroup)
	if !hasRoot {
		return
	}

	p.activeQ.underLock(func(unlockedActiveQ unlockedActiveQueuer) {
		entity := p.getEntityFromAnyQueue(unlockedActiveQ, rootInfoLookup)
		if entity == nil {
			return
		}
		rootInfo := entity.(*framework.QueuedPodGroupInfo)
		rootInfo.UpdatePodGroup(podGroup)
		logger.V(5).Info("Pod Group in the scheduling queue was updated for the root group", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "podGroup", klog.KObj(podGroup))
	})
}

// DeletePodGroup 从队列移除一个 PodGroup 对象，
// 并将与该 PodGroup 关联的所有 Pod 移入 incompletePodGroupPods。
func (p *PriorityQueue) DeletePodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup) {
	p.lock.Lock()
	defer p.lock.Unlock()

	rootInfoLookup, hadRoot := p.workloadForest.getRootLookupInfoForPodGroup(podGroup)
	p.workloadForest.deletePodGroup(podGroup)
	if !hadRoot {
		return
	}

	entity, strategy := p.deleteFromAnyQueue(rootInfoLookup)
	if entity == nil {
		if !p.activeQ.isLastPoppedEntity(rootInfoLookup) {
			// 根 PodGroup/CompositePodGroup 既不 pending 也未入队，
			// 队列中无需跟踪其 Pod：它们要么已调度，要么该组已无 Pod。
			return
		}
		// 取出 pending Pod 并移入 incompletePodGroupPods。
		pendingPods := p.pendingPodGroupPods.clear(podGroup.Namespace, podGroup.Name)
		if len(pendingPods) == 0 {
			// 没有 pending Pod，无需搬移。
			return
		}
		for _, pInfo := range pendingPods {
			p.incompletePodGroupPods.add(pInfo)
		}
		logger.V(5).Info("Pod group deleted, decomposed and enqueued pending pods for the pod group to incompletePodGroupPods", "podGroup", klog.KObj(podGroup), "pods", len(pendingPods))
		return
	}

	rootInfo := entity.(*framework.QueuedPodGroupInfo)
	removedPods := rootInfo.RemovePodGroup(podGroup)
	for _, pInfo := range removedPods {
		p.incompletePodGroupPods.add(pInfo)
	}

	if !rootInfo.HasQueuedPodInfos() {
		logger.V(5).Info("Pod group deleted from root group, decomposed and enqueued pods to incompletePodGroupPods", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "podGroup", klog.KObj(podGroup), "pods", len(removedPods))
		return
	}

	queue := p.requeueEntityWithQueueingStrategy(logger, rootInfo, strategy, framework.EventUnscheduledPodUpdate.Label())
	if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
		p.activeQ.broadcast()
	}
	logger.V(5).Info("Pod group deleted from root group, decomposed and enqueued pods to incompletePodGroupPods", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "podGroup", klog.KObj(podGroup), "pods", len(removedPods))
}

// AddCompositePodGroup 向队列新增一个 CompositePodGroup 对象。
// 它会将与该组合 PodGroup 关联的所有 Pod 重新入队。
func (p *PriorityQueue) AddCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup) {
	if !p.isCompositePodGroupEnabled {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	p.workloadForest.addCompositePodGroup(cpg)
	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForCPG(cpg)
	if !hasRoot {
		// 根节点不存在，Pod 应继续留在 incompletePodGroupPods 等待。
		return
	}

	p.activeQ.underLock(func(unlockedActiveQ unlockedActiveQueuer) {
		entity := p.getEntityFromAnyQueue(unlockedActiveQ, rootInfoLookup)
		if entity == nil {
			return
		}

		rootInfo := entity.(*framework.QueuedPodGroupInfo)
		subtree := p.workloadForest.buildPodGroupInfoForCPG(logger, cpg, sets.New[fwk.EntityKey]())
		rootInfo.AddCompositePodGroup(cpg, subtree)
		logger.V(5).Info("Composite Pod Group added to the existing root group", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "podGroup", klog.KObj(cpg))
	})

	pgs := p.workloadForest.getLeafPodGroups(logger, rootInfoLookup)
	var pInfos []*framework.QueuedPodInfo
	for _, pg := range pgs {
		pInfos = append(pInfos, p.incompletePodGroupPods.clear(pg.Namespace, pg.Name)...)
	}

	for _, pInfo := range pInfos {
		// 统一走 addPodGroupMember 处理，可靠覆盖所有情况。
		p.addPodGroupMember(logger, pInfo)
	}
	logger.V(5).Info("Composite Pod Group was added to the root group and its pods were attempted to be moved from incompletePodGroupPods to the queue", "rootType", rootInfoLookup.Type(), "root", klog.KObj(rootInfoLookup), "compositePodGroup", klog.KObj(cpg), "podsLen", len(pInfos))
}

// UpdateCompositePodGroup 更新队列中已有的 CompositePodGroup 对象。
func (p *PriorityQueue) UpdateCompositePodGroup(logger klog.Logger, newCPG *schedulingv1alpha3.CompositePodGroup) {
	if !p.isCompositePodGroupEnabled {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	p.workloadForest.updateCompositePodGroup(newCPG)
	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForCPG(newCPG)
	if !hasRoot {
		return
	}

	p.activeQ.underLock(func(unlockedActiveQ unlockedActiveQueuer) {
		entity := p.getEntityFromAnyQueue(unlockedActiveQ, rootInfoLookup)
		if entity == nil {
			return
		}
		rootInfo := entity.(*framework.QueuedPodGroupInfo)
		rootInfo.UpdateCompositePodGroup(newCPG)
		logger.V(5).Info("Composite Pod Group in the scheduling queue was updated for the root group", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "compositePodGroup", klog.KObj(newCPG))
	})
}

// DeleteCompositePodGroup 从队列移除一个 CompositePodGroup 对象。
// 它会将该组合 PodGroup 层级下的所有 Pod 移入 incompletePodGroupPods。
func (p *PriorityQueue) DeleteCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup) {
	if !p.isCompositePodGroupEnabled {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	rootInfoLookup, hadRoot := p.workloadForest.getRootLookupInfoForCPG(cpg)
	if !hadRoot {
		p.workloadForest.deleteCompositePodGroup(cpg)
		return
	}
	leafPGs := p.workloadForest.getLeafPodGroups(logger, rootInfoLookup)
	p.workloadForest.deleteCompositePodGroup(cpg)

	entity, strategy := p.deleteFromAnyQueue(rootInfoLookup)
	if entity == nil {
		if !p.activeQ.isLastPoppedEntity(rootInfoLookup) {
			// 根 PodGroup/CompositePodGroup 既不 pending 也未入队，
			// 队列中无需跟踪其 Pod：它们要么已调度，要么该组已无 Pod。
			return
		}
		// 取出 pending Pod 并移入 incompletePodGroupPods。
		var pendingPods []*framework.QueuedPodInfo
		for _, pg := range leafPGs {
			pendingPods = append(pendingPods, p.pendingPodGroupPods.clear(pg.Namespace, pg.Name)...)
		}
		if len(pendingPods) == 0 {
			// 没有 pending Pod，无需搬移。
			return
		}
		for _, pInfo := range pendingPods {
			_, staysPending := p.workloadForest.getRootLookupInfoForPod(pInfo.Pod)
			if staysPending {
				p.pendingPodGroupPods.add(pInfo)
			} else {
				p.incompletePodGroupPods.add(pInfo)
			}
		}
		logger.V(5).Info("Composite pod group deleted from the root group, enqueued pending pods in the composite pod group subtree to incompletePodGroupPods", "rootType", rootInfoLookup.Type(), "root", klog.KObj(rootInfoLookup), "compositePodGroup", klog.KObj(cpg), "pods", len(pendingPods))
		return
	}

	rootInfo := entity.(*framework.QueuedPodGroupInfo)
	removedPods := rootInfo.RemoveCompositePodGroup(cpg)
	for _, pInfo := range removedPods {
		p.incompletePodGroupPods.add(pInfo)
	}

	logger.V(5).Info("Composite pod group deleted from the root group, decomposed and enqueued pods in the composite pod group subtree to incompletePodGroupPods", "rootType", rootInfo.Type(), "root", klog.KObj(rootInfo), "compositePodGroup", klog.KObj(cpg), "pods", len(removedPods))
	if !rootInfo.HasQueuedPodInfos() {
		return
	}

	queue := p.requeueEntityWithQueueingStrategy(logger, rootInfo, strategy, framework.EventUnscheduledPodUpdate.Label())
	if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
		p.activeQ.broadcast()
	}
}

// 注意：调用方必须已持有锁。
// moveAllToActiveOrBackoffQueue 将 unschedulableEntities 中的所有 Pod 移入 activeQ 或 backoffQ。
// 先批量入队再发信号，确保等待中的 Pop() 在所有 Pod 入队完毕、队首为最高优先级时才被唤醒。
func (p *PriorityQueue) moveAllToActiveOrBackoffQueue(logger klog.Logger, event fwk.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck) {
	if !p.isEventOfInterest(logger, event) {
		// 无插件关注该事件，提前返回，避免遍历所有实体执行 preCheck。
		return
	}

	entities, hintKeys := p.collectEntitiesToEvaluate(logger, event, oldObj, newObj, preCheck)
	p.moveEntitiesToActiveOrBackoffQueue(logger, entities, event, oldObj, newObj, hintKeys)
}

// collectEntitiesToEvaluate 根据事件确定哪些不可调度实体需要评估是否重新入队。
// 若 PreQueueingHintFns 收窄了范围，只返回目标实体；否则返回所有通过 preCheck 的实体。
//
// 注意：调用方必须已持有锁。
func (p *PriorityQueue) collectEntitiesToEvaluate(logger klog.Logger, event fwk.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck) ([]framework.QueuedEntityInfo, *preQueueingHintPodKeys) {
	// 运行 PreQueueingHintFns 收窄 Pod 范围。
	hintKeys := p.runPreQueueingHintPlugins(logger, event, oldObj, newObj)

	// TODO: 为 CompositePodGroup 实体增加 PreQueueingHint 支持。
	// 在此之前，启用 CPG 时跳过收窄，避免漏掉 CPG 实体。
	if !p.isCompositePodGroupEnabled && hintKeys != nil {
		logger.V(5).Info("PreQueueingHint narrowed pod set", "candidates", hintKeys.candidatePods.Len(), "total", len(p.unschedulableEntities.entityInfoMap))
		// 直接按 PreQueueingHint 给出的 Pod 查找对应实体。
		entities := make([]framework.QueuedEntityInfo, 0, hintKeys.candidatePods.Len())
		seen := make(map[string]bool)
		for nn := range hintKeys.candidatePods {
			entityKey := fwk.PodKey(nn.Namespace, nn.Name).String()
			if entity, exists := p.unschedulableEntities.entityInfoMap[entityKey]; exists {
				if preCheck == nil || preCheck(entity.(*framework.QueuedPodInfo).Pod) {
					entities = append(entities, entity)
				}
				continue
			}
			// 作为独立 Pod 未找到时，尝试通过 schedulingGroup 字段查找其所属 PodGroup。
			// TODO: 增加对 CompositePodGroup (CPG) 查找的支持。
			if p.isGenericWorkloadEnabled {
				pod, err := p.podLister.Pods(nn.Namespace).Get(nn.Name)
				if err != nil {
					logger.V(5).Info("Failed to get pod for PodGroup lookup", "pod", nn, "err", err)
				} else if pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil {
					entityKey = fwk.PodGroupKey(nn.Namespace, *pod.Spec.SchedulingGroup.PodGroupName).String()
					if entity, exists := p.unschedulableEntities.entityInfoMap[entityKey]; exists && !seen[entityKey] {
						seen[entityKey] = true
						// 只对 PreQueueingHint 命中的那个 Pod 做 preCheck，而非整个 PodGroup 的所有成员。
						if preCheck == nil || preCheck(pod) {
							entities = append(entities, entity)
						}
					}
				}
			}
		}
		return entities, hintKeys
	}

	// 未收窄范围，评估所有不可调度实体。
	entities := make([]framework.QueuedEntityInfo, 0, len(p.unschedulableEntities.entityInfoMap))
	for _, entity := range p.unschedulableEntities.entityInfoMap {
		entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
			if preCheck == nil || preCheck(pInfo.Pod) {
				entities = append(entities, entity)
				return false
			}
			return true
		})
	}
	return entities, nil
}

// MoveAllToActiveOrBackoffQueue 将 unschedulableEntities 中的所有 Pod 移入 activeQ 或 backoffQ。
// 先批量入队再发信号，确保等待中的 Pop() 在所有 Pod 入队完毕、队首为最高优先级时才被唤醒。
func (p *PriorityQueue) MoveAllToActiveOrBackoffQueue(logger klog.Logger, event fwk.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.moveAllToActiveOrBackoffQueue(logger, event, oldObj, newObj, preCheck)
}

// requeueEntityWithQueueingStrategy 根据调度提示尝试将实体重入 activeQ、backoffQ 或不可调度池。
// 返回值是实体最终进入的队列名。
//
// 注意：调用方必须已持有锁。
func (p *PriorityQueue) requeueEntityWithQueueingStrategy(logger klog.Logger, entity framework.QueuedEntityInfo, strategy queueingStrategy, event string) string {
	if strategy == queueSkip {
		// 已存在的实体需要传入当前 gating 状态；新实体会被 addPod/addPodGroup 忽略该参数。
		p.unschedulableEntities.addOrUpdate(entity, entity.Gated(), event, &strategy)
		return unschedulableQ
	}

	// 实体停留在 unschedulableEntities 期间退避可能已结束，移入 backoffQ 前需再检查一次。
	if strategy == queueAfterBackoff && p.backoffQ.isEntityBackingoff(entity) {
		if added := p.moveToBackoffQ(logger, entity, event, &strategy); added {
			return backoffQ
		}
		return unschedulableQ
	}

	// 走到这里说明提示为 queueImmediately，或提示为 queueAfterBackoff 但退避已结束。
	if added := p.moveToActiveQ(logger, entity, event, false, &strategy); added {
		return activeQ
	}
	// 实体被 gating。moveToActiveQ 已将其放回不可调度队列，无需重复操作。
	return unschedulableQ
}

// 注意：调用方必须已持有锁。
func (p *PriorityQueue) moveEntitiesToActiveOrBackoffQueue(logger klog.Logger, entityInfoList []framework.QueuedEntityInfo, event fwk.ClusterEvent, oldObj, newObj interface{}, hintKeys *preQueueingHintPodKeys) {
	if !p.isEventOfInterest(logger, event) {
		// 无插件关注该事件。
		return
	}

	activated := false
	for _, entity := range entityInfoList {
		// 优化：对与 gating 插件无关的事件，跳过被 gating 实体的重新评估。
		// 但通配事件（如定期 flush）始终触发重新评估，避免队列提示不完整或不准确时 Pod 被卡住。
		if entity.Gated() && !framework.ClusterEventIsWildCard(event) && !framework.MatchAnyClusterEvent(event, entity.GetGatingPluginEvents()) {
			// 该事件与该 Pod 的 gating 插件无关，不可能把它移入 activeQ。
			continue
		}

		schedulingHint := p.isEntityWorthRequeuing(logger, entity, event, oldObj, newObj, hintKeys)
		if schedulingHint == queueSkip {
			// QueueingHintFn 判定该事件不值得把实体放入 activeQ 或 backoffQ。
			logger.V(5).Info("Event is not making entity schedulable", "type", entity.Type(), "entity", entity, "event", event.Label())
			continue
		}

		p.unschedulableEntities.delete(entity, entity.Gated())
		queue := p.requeueEntityWithQueueingStrategy(logger, entity, schedulingHint, event.Label())
		if queue == activeQ || (p.isPopFromBackoffQEnabled && queue == backoffQ) {
			activated = true
		}
	}

	// 之后 in-flight Pod 可能调用 AddUnschedulablePodIfNotPresent，
	// 它需要知道调度期间是否观察到过事件。
	if added := p.activeQ.addEventIfAnyInFlight(oldObj, newObj, event); added {
		logger.V(5).Info("Event received while entities are in flight", "event", event.Label())
	}

	if activated {
		p.activeQ.broadcast()
	}
}

// PodsInActiveQ 返回 activeQ 中的所有 Pod。
func (p *PriorityQueue) PodsInActiveQ() []*v1.Pod {
	return p.activeQ.list()
}

// PodsInBackoffQ 返回 backoffQ 中的所有 Pod。
func (p *PriorityQueue) PodsInBackoffQ() []*v1.Pod {
	return p.backoffQ.list()
}

// UnschedulablePods 返回所有处于不可调度状态的 Pod。
func (p *PriorityQueue) UnschedulablePods() []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	var result []*v1.Pod
	for _, entity := range p.unschedulableEntities.entityInfoMap {
		entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
			result = append(result, pInfo.Pod)
			return true
		})
	}
	return result
}

// PendingPodGroupPods 返回所有等待其所属 PodGroup 的 pending Pod。
func (p *PriorityQueue) PendingPodGroupPods() []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pendingPodGroupPods.list()
}

// IncompletePodGroupPodsPods 返回 incompletePodGroupPods 中的所有 pending Pod。
// 仅用于测试。
func (p *PriorityQueue) IncompletePodGroupPodsPods() []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.incompletePodGroupPods.list()
}

// GetPod 在 activeQ、backoffQ 和 unschedulableEntities 中查找 Pod。
func (p *PriorityQueue) GetPod(name, namespace string, schedulingGroup *v1.PodSchedulingGroup) (*framework.QueuedPodInfo, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			SchedulingGroup: schedulingGroup,
		},
	}
	var pInfo *framework.QueuedPodInfo
	p.activeQ.underRLock(func(unlockedActiveQ unlockedActiveQueueReader) {
		pInfo = p.getPod(pod, unlockedActiveQ)
	})
	return pInfo, pInfo != nil
}

// GetPodGroup 在 activeQ、backoffQ 和 unschedulableEntities 中查找 PodGroup。
// 该函数仅用于测试。
func (p *PriorityQueue) GetPodGroup(name, namespace string, entityKeyType fwk.EntityKeyType) (*framework.QueuedPodGroupInfo, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	pgInfo := &framework.QueuedPodGroupInfo{
		PodGroupInfo: &framework.PodGroupInfo{
			Name:      name,
			Namespace: namespace,
			Type:      entityKeyType,
		},
	}
	var foundPGInfo *framework.QueuedPodGroupInfo
	p.activeQ.underRLock(func(unlockedActiveQ unlockedActiveQueueReader) {
		entity := p.getEntityFromAnyQueue(unlockedActiveQ, pgInfo)
		if entity != nil {
			if pg, ok := entity.(*framework.QueuedPodGroupInfo); ok {
				foundPGInfo = pg
			}
		}
	})
	return foundPGInfo, foundPGInfo != nil
}

func (p *PriorityQueue) getPod(podLookup *v1.Pod, unlockedActiveQ unlockedActiveQueueReader) *framework.QueuedPodInfo {
	var entityLookup framework.QueuedEntityInfo
	if p.isPodGroupMember(podLookup) {
		var hasRoot bool
		entityLookup, hasRoot = p.workloadForest.getRootLookupInfoForPod(podLookup)
		if !hasRoot {
			return p.incompletePodGroupPods.get(podLookup)
		}
	} else {
		entityLookup = newQueuedPodInfoForLookup(podLookup)
	}

	entity := p.getEntityFromAnyQueue(unlockedActiveQ, entityLookup)
	if entity == nil {
		if !p.isPodGroupMember(podLookup) {
			return nil
		}
		return p.pendingPodGroupPods.get(podLookup)
	}
	var foundPodInfo *framework.QueuedPodInfo
	entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
		if pInfo.Pod.Name == podLookup.Name && pInfo.Pod.Namespace == podLookup.Namespace {
			foundPodInfo = pInfo
			return false
		}
		return true
	})
	return foundPodInfo
}

var pendingPodsSummary = "activeQ:%v; backoffQ:%v; unschedulableEntities:%v"
var pendingPodsExtendedSummary = "activeQ:%v; backoffQ:%v; unschedulableEntities:%v; pendingPodGroupMemberPods:%v; incompletePodGroupPods:%v"

// PendingPods 返回队列中所有 pending Pod，并附带一个调试字符串，
// 记录各队列中的 Pod 数量。
// 该函数用于 scheduler cache dumper 和 comparer 的调试。
func (p *PriorityQueue) PendingPods() ([]*v1.Pod, string) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	result := p.PodsInActiveQ()
	activeQLen := len(result)
	backoffQPods := p.PodsInBackoffQ()
	backoffQLen := len(backoffQPods)
	result = append(result, backoffQPods...)
	unschedulablePodsLen := 0
	for _, entity := range p.unschedulableEntities.entityInfoMap {
		entity.ForEachPodInfo(func(pInfo *framework.QueuedPodInfo) bool {
			result = append(result, pInfo.Pod)
			unschedulablePodsLen++
			return true
		})
	}
	if !p.isGenericWorkloadEnabled {
		return result, fmt.Sprintf(pendingPodsSummary, activeQLen, backoffQLen, unschedulablePodsLen)
	}
	incompletePodGroupPodsPodsLen := p.incompletePodGroupPods.len()
	result = append(result, p.incompletePodGroupPods.list()...)
	pendingPodGroupPodsLen := p.pendingPodGroupPods.len()
	result = append(result, p.pendingPodGroupPods.list()...)
	return result, fmt.Sprintf(pendingPodsExtendedSummary, activeQLen, backoffQLen, unschedulablePodsLen, pendingPodGroupPodsLen, incompletePodGroupPodsPodsLen)
}

// PatchPodStatus 通过 API dispatcher 发送更新 API 调用来处理 Pod 状态更新。
// 仅在 SchedulerAsyncAPICalls feature gate 启用时才应使用该方法。
func (p *PriorityQueue) PatchPodStatus(pod *v1.Pod, conditions []*v1.PodCondition, nominatingInfo *fwk.NominatingInfo) (<-chan error, error) {
	// 不在缓存中保存任何内容，后续版本可能扩展此处行为。
	onFinish := make(chan error, 1)
	err := p.apiDispatcher.Add(apicalls.Implementations.PodStatusPatch(pod, conditions, nominatingInfo), fwk.APICallOptions{
		OnFinish: onFinish,
	})
	if fwk.IsUnexpectedError(err) {
		return onFinish, err
	}
	return onFinish, nil
}

// 注意：调用方必须同时持有 p.lock.RLock 和 p.activeQ.getLock().RLock。
func (p *PriorityQueue) nominatedPodToInfo(np podRef, unlockedActiveQ unlockedActiveQueueReader) *framework.PodInfo {
	pod := np.toPod()
	pInfo := p.getPod(pod, unlockedActiveQ)
	if pInfo != nil {
		return pInfo.PodInfo
	}
	return &framework.PodInfo{Pod: pod}
}

// getEntityFromAnyQueue 从 activeQ、backoffQ 或 unschedulableEntities 中返回实体。
// 若任何队列中都没有该实体，则返回 nil。
// 注意：若调用方修改了实体的大小，应自行同步更新队列指标以匹配新大小。
func (p *PriorityQueue) getEntityFromAnyQueue(unlockedActiveQ unlockedActiveQueueReader, entityLookup framework.QueuedEntityInfo) framework.QueuedEntityInfo {
	existing, ok := unlockedActiveQ.get(entityLookup)
	if ok {
		return existing
	}
	existing, ok = p.backoffQ.get(entityLookup)
	if ok {
		return existing
	}
	if existing = p.unschedulableEntities.get(entityLookup); existing != nil {
		if existing.Gated() {
			return existing
		}
		return existing
	}
	return nil
}

// Close 关闭优先级队列。
func (p *PriorityQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.stop)
	p.activeQ.close()
	p.activeQ.broadcast()
}

// NominatedPodsForNode 返回被提名在给定节点上运行、但仍在等待其他 Pod 从该节点移除的 Pod 副本。
// 注意：任何情况下都不要在持有队列锁时调用该函数。
func (p *PriorityQueue) NominatedPodsForNode(nodeName string) []fwk.PodInfo {
	p.lock.RLock()
	defer p.lock.RUnlock()
	nominatedPods := p.nominator.nominatedPodsForNode(nodeName)
	if len(nominatedPods) == 0 {
		return []fwk.PodInfo{}
	}
	pods := make([]fwk.PodInfo, len(nominatedPods))
	p.activeQ.underRLock(func(unlockedActiveQ unlockedActiveQueueReader) {
		for i, np := range nominatedPods {
			pods[i] = p.nominatedPodToInfo(np, unlockedActiveQ).DeepCopy()
		}
	})
	return pods
}

// signPod 在 OpportunisticBatching 启用时为给定 Pod 计算调度签名。
// 该签名用于缓存和复用相同 Pod 的调度结果。
func (p *PriorityQueue) signPod(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
	if !p.isOpportunisticBatchingEnabled {
		return nil
	}

	if p.podSigners == nil {
		return nil
	}

	signer, ok := p.podSigners[pod.Spec.SchedulerName]
	if !ok {
		utilruntime.HandleErrorWithContext(ctx, nil, "No signer registered for scheduler profile", "pod", klog.KObj(pod), "schedulerName", pod.Spec.SchedulerName)
		return nil
	}

	return signer(ctx, pod)
}

// newQueuedPodInfo 构造一个 QueuedPodInfo 对象。
func (p *PriorityQueue) newQueuedPodInfo(ctx context.Context, pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	now := p.clock.Now()
	// 忽略该错误：apiserver 未正确校验亲和性条款，且出于向后兼容无法修复校验逻辑。
	podInfo, _ := framework.NewPodInfo(pod)
	return &framework.QueuedPodInfo{
		PodInfo: podInfo,
		QueueingParams: framework.QueueingParams{
			Timestamp:               now,
			InitialAttemptTimestamp: nil,
			UnschedulablePlugins:    sets.New(plugins...),
		},
		PodSignature: p.signPod(ctx, pod),
	}
}

// recordIncomingEntitiesMetrics 记录 Pod 级别和实体级别（单个 Pod 或 PodGroup）的入队指标。
// strategy 是此前的排队策略，用于判断实体是跨队列移动还是新增，以正确维护入队指标。
// strategy 为 nil 表示实体是新增的；若此前策略对应的队列标签与目标队列标签不同，则说明实体发生了跨队列移动。
// 这两种情况都要为目标队列增加入队实体指标。
func recordIncomingEntitiesMetrics(targetQueueLabel string, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy) {
	metrics.SchedulerQueueIncomingPods.WithLabelValues(targetQueueLabel, event).Add(float64(entity.Size()))

	if strategy == nil || targetQueueLabel != strategyToQueueLabel(*strategy) {
		if entityLabel, ok := metrics.EntityToLabel(entity); ok {
			metrics.SchedulerQueueIncomingEntities.WithLabelValues(targetQueueLabel, event, entityLabel).Inc()
		}
	}
}

// strategyToQueueLabel 将此前的 queueingStrategy 映射到实体当时所在队列的指标标签。
func strategyToQueueLabel(strategy queueingStrategy) string {
	switch strategy {
	case queueImmediately:
		return "active"
	case queueAfterBackoff:
		return "backoff"
	default:
		return "unschedulable"
	}
}

// queuedEntityKeyFunc 根据实体的类型、命名空间和名称生成唯一键。
func queuedEntityKeyFunc(obj framework.QueuedEntityInfo) string {
	return fmt.Sprintf("%s/%s/%s", obj.Type(), obj.GetNamespace(), obj.GetName())
}

// podKey 返回 Pod 对应的实体键。
func podKey(pod *v1.Pod) fwk.EntityKey {
	return fwk.PodKey(pod.Namespace, pod.Name)
}

// podGroupKeyForPod 返回 Pod 所属 PodGroup 的实体键。
func podGroupKeyForPod(pod *v1.Pod) fwk.EntityKey {
	return fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
}

// podGroupKey 返回 PodGroup 对应的实体键。
func podGroupKey(podGroup *schedulingv1beta1.PodGroup) fwk.EntityKey {
	return fwk.PodGroupKey(podGroup.Namespace, podGroup.Name)
}

// compositePodGroupKey 返回 CompositePodGroup 对应的实体键。
func compositePodGroupKey(cpg *schedulingv1alpha3.CompositePodGroup) fwk.EntityKey {
	return fwk.CompositePodGroupKey(cpg.Namespace, cpg.Name)
}

// preQueueingHintPodKeys 保存针对某事件运行 PreQueueingHintFns 的结果。
// candidatePods 是所有插件希望重新评估的 Pod 的并集。
type preQueueingHintPodKeys struct {
	candidatePods sets.Set[types.NamespacedName]
	perPlugin     map[string]sets.Set[types.NamespacedName]
}

// runPreQueueingHintPlugins 针对给定事件运行所有已注册的 PreQueueingHintFn，
// 返回收窄后的待评估 Pod 键集合；若应评估所有 Pod，则返回 nil。
//
// 注意：调用方必须已持有锁。
func (p *PriorityQueue) runPreQueueingHintPlugins(logger klog.Logger, event fwk.ClusterEvent, oldObj, newObj interface{}) *preQueueingHintPodKeys {
	if !p.isPreQueueingHintsEnabled {
		return nil
	}
	if oldObj == nil && newObj == nil {
		return nil
	}
	if framework.ClusterEventIsWildCard(event) {
		return nil
	}
	result := &preQueueingHintPodKeys{
		perPlugin: make(map[string]sets.Set[types.NamespacedName]),
	}
	for _, hintMap := range p.queueingHintMap {
		for eventToMatch, hintfns := range hintMap {
			if !framework.MatchClusterEvents(eventToMatch, event) {
				continue
			}
			for _, hintfn := range hintfns {
				if hintfn.PreQueueingHintFn == nil {
					continue
				}
				hintResult, err := hintfn.PreQueueingHintFn(logger, oldObj, newObj)
				if err != nil {
					logger.Error(err, "PreQueueingHintFn failed, falling back to evaluate all pods", "plugin", hintfn.PluginName)
					return nil
				}
				if hintResult.AllPods {
					// 该插件要求评估所有 Pod，无法收窄范围。
					p.metricsRecorder.ObserveCounterAsync(metrics.PreQueueingHintEvaluations, 1, hintfn.PluginName, "all_pods")
					return nil
				}
				p.metricsRecorder.ObserveCounterAsync(metrics.PreQueueingHintEvaluations, 1, hintfn.PluginName, "narrowed")
				keys := sets.New(hintResult.Pods...)
				if existing, ok := result.perPlugin[hintfn.PluginName]; ok {
					result.perPlugin[hintfn.PluginName] = existing.Union(keys)
				} else {
					result.perPlugin[hintfn.PluginName] = keys
				}
				if result.candidatePods == nil {
					result.candidatePods = keys
				} else {
					result.candidatePods = result.candidatePods.Union(keys)
				}
			}
		}
	}
	if result.candidatePods == nil {
		return nil
	}
	return result
}
