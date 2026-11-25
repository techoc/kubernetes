/*
Copyright 2014 The Kubernetes Authors.

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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/informers/internalinterfaces"
	clientset "k8s.io/client-go/kubernetes"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1alpha3"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	resourceslicetracker "k8s.io/dynamic-resource-allocation/resourceslice/tracker"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	apicache "k8s.io/kubernetes/pkg/scheduler/backend/api_cache"
	apidispatcher "k8s.io/kubernetes/pkg/scheduler/backend/api_dispatcher"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	cachedebugger "k8s.io/kubernetes/pkg/scheduler/backend/cache/debugger"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	frameworkplugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodevolumelimits"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"k8s.io/utils/clock"
)

// ErrNoNodesAvailable 表示没有可用节点来调度 Pod。
var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

// Scheduler 监听新出现的未调度 Pod，为其寻找合适的节点，并把绑定结果写回 apiserver。
type Scheduler struct {
	// 通过 Cache 做出的改动，应当能被 NodeLister 与 Algorithm 观察到。
	Cache internalcache.Cache

	// Extenders 保存可参与过滤、抢占、打分和绑定的调度扩展器。
	Extenders []fwk.Extender

	// NextEntity 应阻塞直到下一个实体（Pod 或 PodGroup）可用。
	// 这里不用 channel，是因为调度一个实体可能耗时较长，
	// 实体若堆积在 channel 中会因等待而变得陈旧。
	NextEntity func(logger klog.Logger) (framework.QueuedEntityInfo, error)

	// FailureHandler 在调度失败时调用。
	FailureHandler FailureHandlerFn

	// SchedulePod 尝试把给定 Pod 调度到节点列表中的某个节点。
	// 成功时返回包含推荐主机名的 ScheduleResult，失败时返回带原因的 FitError。
	SchedulePod func(ctx context.Context, fwk framework.Framework, state fwk.CycleState, podInfo *framework.QueuedPodInfo) (ScheduleResult, error)

	// 关闭此通道即可关停调度器。
	StopEverything <-chan struct{}

	// SchedulingQueue 保存待调度的 Pod。
	SchedulingQueue internalqueue.SchedulingQueue

	// 如有可能，应优先通过调度队列等间接方式操作 APIDispatcher。
	// 仅当 SchedulerAsyncAPICalls 特性门关闭时该字段才为 nil。
	// 树内代码不应直接向 APIDispatcher 添加调用，
	// 应改走 framework.APICache。
	APIDispatcher *apidispatcher.APIDispatcher

	// Profiles 是调度配置文件的集合。
	Profiles profile.Map

	client clientset.Interface

	compositePodGroupLister schedulinglisters.CompositePodGroupLister

	nodeInfoSnapshot *internalcache.Snapshot

	percentageOfNodesToScore int32

	nextStartNodeIndex int

	// logger 在创建 Scheduler 时必须初始化，
	// 否则日志函数会访问 nil sink 并 panic。
	logger klog.Logger

	// registeredHandlers 保存所有事件处理器的注册句柄，
	// 用于在调度周期开始前确认它们都已完成同步。
	registeredHandlers []cache.ResourceEventHandlerRegistration

	nominatedNodeNameForExpectationEnabled              bool
	genericWorkloadEnabled                              bool
	inPlacePodVerticalScalingSchedulerPreemptionEnabled bool
}

// applyDefaultHandlers 把调度与失败处理挂载到默认实现上。
func (sched *Scheduler) applyDefaultHandlers() {
	sched.SchedulePod = sched.schedulePod
	sched.FailureHandler = sched.handleSchedulingFailure
}

// schedulerOptions 保存 New 可覆盖的调度器构建参数。
type schedulerOptions struct {
	clock                  clock.WithTicker
	componentConfigVersion string
	kubeConfig             *restclient.Config
	// 若在 v1 配置中设置了 profile 级别的 percentageOfNodesToScore，此处的值会被覆盖。
	percentageOfNodesToScore          int32
	podInitialBackoffSeconds          int64
	podMaxBackoffSeconds              int64
	podMaxInUnschedulablePodsDuration time.Duration
	// 保存待与树内注册表合并的树外插件。
	frameworkOutOfTreeRegistry frameworkruntime.Registry
	profiles                   []schedulerapi.KubeSchedulerProfile
	extenders                  []schedulerapi.Extender
	frameworkCapturer          FrameworkCapturer
	parallelism                int32
	applyDefaultProfile        bool
	nodeInfoSnapshot           *internalcache.Snapshot
}

// Option 用于配置 Scheduler 的构建参数。
type Option func(*schedulerOptions)

// ScheduleResult 表示一次 Pod 调度的结果。
type ScheduleResult struct {
	// 被选中节点的名称。
	SuggestedHost string
	// 调度器在过滤及后续阶段评估该 Pod 时检查的节点数。
	EvaluatedNodes int
	// 被评估节点中适合该 Pod 的节点数。
	FeasibleNodes int
	// 本次调度周期的提名信息。
	nominatingInfo *fwk.NominatingInfo
}

// WithComponentConfigVersion 设置所使用的组件配置版本。
// 参数应为转换来源外部类型的完整 scheme group/version
// （例如 "kubescheduler.config.k8s.io/v1"）。
func WithComponentConfigVersion(apiVersion string) Option {
	return func(o *schedulerOptions) {
		o.componentConfigVersion = apiVersion
	}
}

// WithKubeConfig 为 Scheduler 设置 kube 配置。
func WithKubeConfig(cfg *restclient.Config) Option {
	return func(o *schedulerOptions) {
		o.kubeConfig = cfg
	}
}

// WithProfiles 为 Scheduler 设置调度配置文件。
// 默认只有一个名为 "default-scheduler" 的配置文件。
func WithProfiles(p ...schedulerapi.KubeSchedulerProfile) Option {
	return func(o *schedulerOptions) {
		o.profiles = p
		o.applyDefaultProfile = false
	}
}

// WithParallelism 设置所有调度算法的并行度，默认值为 16。
func WithParallelism(threads int32) Option {
	return func(o *schedulerOptions) {
		o.parallelism = threads
	}
}

// WithPercentageOfNodesToScore 设置 Scheduler 的 percentageOfNodesToScore。
// 默认值 0 表示使用自适应百分比：50 - (节点数)/125。
func WithPercentageOfNodesToScore(percentageOfNodesToScore *int32) Option {
	return func(o *schedulerOptions) {
		if percentageOfNodesToScore != nil {
			o.percentageOfNodesToScore = *percentageOfNodesToScore
		}
	}
}

// WithFrameworkOutOfTreeRegistry 设置树外插件注册表，这些插件会追加到默认注册表之后。
func WithFrameworkOutOfTreeRegistry(registry frameworkruntime.Registry) Option {
	return func(o *schedulerOptions) {
		o.frameworkOutOfTreeRegistry = registry
	}
}

// WithPodInitialBackoffSeconds 设置 Scheduler 的 Pod 初始退避秒数，默认值为 1。
func WithPodInitialBackoffSeconds(podInitialBackoffSeconds int64) Option {
	return func(o *schedulerOptions) {
		o.podInitialBackoffSeconds = podInitialBackoffSeconds
	}
}

// WithPodMaxBackoffSeconds 设置 Scheduler 的 Pod 最大退避秒数，默认值为 10。
func WithPodMaxBackoffSeconds(podMaxBackoffSeconds int64) Option {
	return func(o *schedulerOptions) {
		o.podMaxBackoffSeconds = podMaxBackoffSeconds
	}
}

// WithPodMaxInUnschedulablePodsDuration 设置 PriorityQueue 中 Pod 停留在不可调度队列的最长时间。
func WithPodMaxInUnschedulablePodsDuration(duration time.Duration) Option {
	return func(o *schedulerOptions) {
		o.podMaxInUnschedulablePodsDuration = duration
	}
}

// WithExtenders 为 Scheduler 设置 extender。
func WithExtenders(e ...schedulerapi.Extender) Option {
	return func(o *schedulerOptions) {
		o.extenders = e
	}
}

// WithClock 为 PriorityQueue 设置时钟，默认使用 clock.RealClock。
func WithClock(clock clock.WithTicker) Option {
	return func(o *schedulerOptions) {
		o.clock = clock
	}
}

// FrameworkCapturer 用于在构建 framework 时注册一个通知回调。
type FrameworkCapturer func(schedulerapi.KubeSchedulerProfile)

// WithBuildFrameworkCapturer 设置回调，以便在构建 framework 时获取其细节。
func WithBuildFrameworkCapturer(fc FrameworkCapturer) Option {
	return func(o *schedulerOptions) {
		o.frameworkCapturer = fc
	}
}

// WithNodeInfoSnapshot 为 Scheduler 设置 nodeInfoSnapshot。
func WithNodeInfoSnapshot(snapshot *internalcache.Snapshot) Option {
	return func(o *schedulerOptions) {
		o.nodeInfoSnapshot = snapshot
	}
}

var defaultSchedulerOptions = schedulerOptions{
	clock:                             clock.RealClock{},
	percentageOfNodesToScore:          schedulerapi.DefaultPercentageOfNodesToScore,
	podInitialBackoffSeconds:          int64(internalqueue.DefaultPodInitialBackoffDuration.Seconds()),
	podMaxBackoffSeconds:              int64(internalqueue.DefaultPodMaxBackoffDuration.Seconds()),
	podMaxInUnschedulablePodsDuration: internalqueue.DefaultPodMaxInUnschedulablePodsDuration,
	parallelism:                       int32(parallelize.DefaultParallelism),
	// 理想情况下可以在此处静态设置默认 profile，但创建默认 profile 可能需要
	// 检查特性门，而特性门在测试中会动态改变。因此推迟到 New 被调用时再创建。
	applyDefaultProfile: true,
}

// New 创建并返回一个 Scheduler。
func New(ctx context.Context,
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	dynInformerFactory dynamicinformer.DynamicSharedInformerFactory,
	recorderFactory profile.RecorderFactory,
	opts ...Option) (*Scheduler, error) {

	logger := klog.FromContext(ctx)
	stopEverything := ctx.Done()

	options := defaultSchedulerOptions
	for _, opt := range opts {
		opt(&options)
	}

	if options.applyDefaultProfile {
		var versionedCfg configv1.KubeSchedulerConfiguration
		scheme.Scheme.Default(&versionedCfg)
		cfg := schedulerapi.KubeSchedulerConfiguration{}
		if err := scheme.Scheme.Convert(&versionedCfg, &cfg, nil); err != nil {
			return nil, err
		}
		options.profiles = cfg.Profiles
	}

	registry := frameworkplugins.NewInTreeRegistry()
	if err := registry.Merge(options.frameworkOutOfTreeRegistry); err != nil {
		return nil, err
	}

	metrics.Register()

	extenders, err := buildExtenders(logger, options.extenders, options.profiles)
	if err != nil {
		return nil, fmt.Errorf("couldn't build extenders: %w", err)
	}

	podLister := informerFactory.Core().V1().Pods().Lister()
	nodeLister := informerFactory.Core().V1().Nodes().Lister()

	snapshot := options.nodeInfoSnapshot
	if snapshot == nil {
		snapshot = internalcache.NewEmptySnapshot()
	}
	metricsRecorder := metrics.NewMetricsAsyncRecorder(1000, time.Second, stopEverything)
	// waitingPods 保存调度器中所有处于 permit 阶段等待的 Pod。
	waitingPods := frameworkruntime.NewWaitingPodsMap()

	// podsInPreBind 保存调度器中所有处于 preBind 阶段的 Pod。
	podsInPreBind := frameworkruntime.NewPodsInPreBindMap()

	var resourceClaimCache *assumecache.AssumeCache
	var resourceSliceTracker *resourceslicetracker.Tracker
	var draManager fwk.SharedDRAManager
	if feature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		resourceClaimInformer := informerFactory.Resource().V1().ResourceClaims().Informer()
		resourceClaimCache = assumecache.NewAssumeCache(logger, resourceClaimInformer, "ResourceClaim", "", nil)
		resourceSliceTrackerOpts := resourceslicetracker.Options{
			EnableDeviceTaintRules:   feature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules),
			EnableConsumableCapacity: feature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity),
			SliceInformer:            informerFactory.Resource().V1().ResourceSlices(),
			KubeClient:               client,
		}
		// 关闭设备污点规则时无需额外的 informer，tracker 退化为对 slice informer 的简单包装。
		if resourceSliceTrackerOpts.EnableDeviceTaintRules {
			resourceSliceTrackerOpts.TaintInformer = informerFactory.Resource().V1().DeviceTaintRules()
		}
		resourceSliceTracker, err = resourceslicetracker.StartTracker(ctx, resourceSliceTrackerOpts)
		if err != nil {
			return nil, fmt.Errorf("couldn't start resource slice tracker: %w", err)
		}
		draManager = dynamicresources.NewDRAManager(ctx, resourceClaimCache, resourceSliceTracker, informerFactory)
	}
	sharedCSIManager := nodevolumelimits.NewCSIManager(informerFactory.Storage().V1().CSINodes().Lister())

	var apiDispatcher *apidispatcher.APIDispatcher
	if feature.DefaultFeatureGate.Enabled(features.SchedulerAsyncAPICalls) {
		apiDispatcher = apidispatcher.New(client, int(options.parallelism), apicalls.Relevances)
	}

	schedulerCache := internalcache.New(ctx, apiDispatcher, feature.DefaultFeatureGate.Enabled(features.GenericWorkload), feature.DefaultFeatureGate.Enabled(features.CompositePodGroup))

	profiles, err := profile.NewMap(ctx, options.profiles, registry, recorderFactory,
		frameworkruntime.WithComponentConfigVersion(options.componentConfigVersion),
		frameworkruntime.WithClientSet(client),
		frameworkruntime.WithKubeConfig(options.kubeConfig),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithSharedDRAManager(draManager),
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
		frameworkruntime.WithCaptureProfile(frameworkruntime.CaptureProfile(options.frameworkCapturer)),
		frameworkruntime.WithParallelism(int(options.parallelism)),
		frameworkruntime.WithExtenders(extenders),
		frameworkruntime.WithMetricsRecorder(metricsRecorder),
		frameworkruntime.WithWaitingPods(waitingPods),
		frameworkruntime.WithPodsInPreBind(podsInPreBind),
		frameworkruntime.WithAPIDispatcher(apiDispatcher),
		frameworkruntime.WithSharedCSIManager(sharedCSIManager),
		frameworkruntime.WithPodGroupManager(schedulerCache),
	)
	if err != nil {
		return nil, fmt.Errorf("initializing profiles: %v", err)
	}

	if len(profiles) == 0 {
		return nil, errors.New("at least one profile is required")
	}

	preEnqueuePluginMap := make(map[string]map[string]fwk.PreEnqueuePlugin)
	queueingHintsPerProfile := make(internalqueue.QueueingHintMapPerProfile)
	var returnErr error
	for profileName, profile := range profiles {
		plugins := profile.PreEnqueuePlugins()
		preEnqueuePluginMap[profileName] = make(map[string]fwk.PreEnqueuePlugin, len(plugins))
		for _, plugin := range plugins {
			preEnqueuePluginMap[profileName][plugin.Name()] = plugin
		}

		queueingHintsPerProfile[profileName], err = buildQueueingHintMap(ctx, profile.EnqueueExtensions())
		if err != nil {
			returnErr = errors.Join(returnErr, err)
		}
	}

	if returnErr != nil {
		return nil, returnErr
	}

	// 从每个 profile 中提取 Pod 签名函数，供调度队列使用。
	podSigners := make(map[string]internalqueue.PodSigner)
	for profileName, profile := range profiles {
		podSigners[profileName] = profile.SignPod
	}

	podQueue := internalqueue.NewSchedulingQueue(
		profiles[options.profiles[0].SchedulerName].QueueSortFunc(),
		informerFactory,
		internalqueue.WithClock(options.clock),
		internalqueue.WithPodInitialBackoffDuration(time.Duration(options.podInitialBackoffSeconds)*time.Second),
		internalqueue.WithPodMaxBackoffDuration(time.Duration(options.podMaxBackoffSeconds)*time.Second),
		internalqueue.WithPodLister(podLister),
		internalqueue.WithPodMaxInUnschedulablePodsDuration(options.podMaxInUnschedulablePodsDuration),
		internalqueue.WithPreEnqueuePluginMap(preEnqueuePluginMap),
		internalqueue.WithQueueingHintMapPerProfile(queueingHintsPerProfile),
		internalqueue.WithPluginMetricsSamplePercent(pluginMetricsSamplePercent),
		internalqueue.WithMetricsRecorder(metricsRecorder),
		internalqueue.WithAPIDispatcher(apiDispatcher),
		internalqueue.WithPodSigners(podSigners),
	)

	var apiCache fwk.APICacher
	if apiDispatcher != nil {
		apiCache = apicache.New(podQueue, schedulerCache)
	}

	for _, fwk := range profiles {
		fwk.SetPodNominator(podQueue)
		fwk.SetPodActivator(podQueue)
		fwk.SetAPICacher(apiCache)
	}

	// 初始化缓存调试器，便于在运行时比对缓存与 informer 状态。
	debugger := cachedebugger.New(nodeLister, podLister, schedulerCache, podQueue)
	debugger.ListenForSignal(ctx)

	var compositePodGroupLister schedulinglisters.CompositePodGroupLister
	if feature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		compositePodGroupLister = informerFactory.Scheduling().V1alpha3().CompositePodGroups().Lister()
	}

	sched := &Scheduler{
		Cache:                                  schedulerCache,
		client:                                 client,
		nodeInfoSnapshot:                       snapshot,
		percentageOfNodesToScore:               options.percentageOfNodesToScore,
		Extenders:                              extenders,
		StopEverything:                         stopEverything,
		SchedulingQueue:                        podQueue,
		Profiles:                               profiles,
		logger:                                 logger,
		APIDispatcher:                          apiDispatcher,
		compositePodGroupLister:                compositePodGroupLister,
		nominatedNodeNameForExpectationEnabled: feature.DefaultFeatureGate.Enabled(features.NominatedNodeNameForExpectation),
		genericWorkloadEnabled:                 feature.DefaultFeatureGate.Enabled(features.GenericWorkload),
		inPlacePodVerticalScalingSchedulerPreemptionEnabled: feature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption),
	}
	sched.NextEntity = podQueue.Pop
	sched.applyDefaultHandlers()

	if err = addAllEventHandlers(sched, informerFactory, dynInformerFactory, resourceClaimCache, resourceSliceTracker, draManager, unionedGVKs(queueingHintsPerProfile)); err != nil {
		return nil, fmt.Errorf("adding event handlers: %w", err)
	}

	return sched, nil
}

// defaultQueueingHintFn 是默认的入队提示函数，总是返回 Queue。
var defaultQueueingHintFn = func(_ klog.Logger, _ *v1.Pod, _, _ interface{}) (fwk.QueueingHint, error) {
	return fwk.Queue, nil
}

// buildQueueingHintMap 汇总各插件注册的事件及其入队提示函数。
func buildQueueingHintMap(ctx context.Context, es []fwk.EnqueueExtensions) (internalqueue.QueueingHintMap, error) {
	queueingHintMap := make(internalqueue.QueueingHintMap)
	var returnErr error
	for _, e := range es {
		events, err := e.EventsToRegister(ctx)
		if err != nil {
			returnErr = errors.Join(returnErr, err)
		}

		// 插件注册空事件列表通常意味着 Pod 只能通过自身更新重新变为可调度
		// （如 schedulingGates 插件），这类 Pod 会经 priorityQueue.Update() 回到 activeQ。
		if len(events) == 0 {
			continue
		}

		// 注意：极少情况下插件实现了 EnqueueExtensions 却返回 nil。
		// 这视为该插件不关注任何事件，被它拒绝的 Pod 无法靠常规集群事件重新入队，
		// 因此这里直接忽略这样的 EventsToRegister。

		for _, event := range events {
			fn := event.QueueingHintFn
			if fn == nil {
				fn = defaultQueueingHintFn
			}

			queueingHintFn := &internalqueue.QueueingHintFunction{
				PluginName:        e.Name(),
				QueueingHintFn:    fn,
				PreQueueingHintFn: event.PreQueueingHintFn,
			}

			if event.Event.Resource == fwk.Pod {
				for _, podEvent := range framework.UnrollPodEvent(event.Event) {
					queueingHintMap[podEvent] = append(queueingHintMap[podEvent], queueingHintFn)
				}
			} else {
				queueingHintMap[event.Event] = append(queueingHintMap[event.Event], queueingHintFn)
			}
		}
	}
	if returnErr != nil {
		return nil, returnErr
	}
	return queueingHintMap, nil
}

// Run 开始监听与调度，会一直阻塞直到 ctx 结束。
func (sched *Scheduler) Run(ctx context.Context) {
	logger := klog.FromContext(ctx)
	sched.SchedulingQueue.Run(logger)

	if sched.APIDispatcher != nil {
		sched.APIDispatcher.Run(logger)
	}

	// scheduleOne 会阻塞等待调度队列中的下一个实体。若没有待调度对象，它会一直挂起；
	// 因此必须放在独立 goroutine 中运行，否则会阻塞当前 goroutine 关闭 SchedulingQueue，
	// 进而在关停时形成死锁。
	go wait.UntilWithContext(ctx, sched.ScheduleOne, 0)

	<-ctx.Done()
	if sched.APIDispatcher != nil {
		sched.APIDispatcher.Close()
	}
	sched.SchedulingQueue.Close()

	// 实现了 io.Closer 的插件会在此被关闭。
	err := sched.Profiles.Close()
	if err != nil {
		logger.Error(err, "Failed to close plugins")
	}
}

// NewInformerFactory 创建 SharedInformerFactory，并初始化调度器专用的 Pod informer。
func NewInformerFactory(cs clientset.Interface, resyncPeriod time.Duration, informerName *cache.InformerName) informers.SharedInformerFactory {
	informerFactory := informers.NewSharedInformerFactoryWithOptions(cs, resyncPeriod, informers.WithInformerName(informerName))
	informerFactory.InformerFor(&v1.Pod{}, func(cs clientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
		return newPodInformer(cs, resyncPeriod, informerName)
	})
	return informerFactory
}

// buildExtenders 构造 HTTP extender，并把被忽略的扩展资源同步到各 profile 的插件参数中。
func buildExtenders(logger klog.Logger, extenders []schedulerapi.Extender, profiles []schedulerapi.KubeSchedulerProfile) ([]fwk.Extender, error) {
	var fExtenders []fwk.Extender
	if len(extenders) == 0 {
		return nil, nil
	}

	var ignoredExtendedResources []string
	var ignorableExtenders []fwk.Extender
	for i := range extenders {
		logger.V(2).Info("Creating extender", "extender", extenders[i])
		extender, err := NewHTTPExtender(&extenders[i])
		if err != nil {
			return nil, err
		}
		if !extender.IsIgnorable() {
			fExtenders = append(fExtenders, extender)
		} else {
			ignorableExtenders = append(ignorableExtenders, extender)
		}
		for _, r := range extenders[i].ManagedResources {
			if r.IgnoredByScheduler {
				ignoredExtendedResources = append(ignoredExtendedResources, r.Name)
			}
		}
	}
	// 可忽略的 extender 放到队尾，让不可忽略者先执行。
	fExtenders = append(fExtenders, ignorableExtenders...)

	// 把 extender 中被忽略的扩展资源写入每个 profile 的 pluginConfig。
	// 这仅对 ComponentConfig 生效——只有它同时支持配置 Extender 和插件参数，
	// 且此时 extender 的忽略资源优先。
	if len(ignoredExtendedResources) == 0 {
		return fExtenders, nil
	}

	for i := range profiles {
		prof := &profiles[i]
		var found = false
		for k := range prof.PluginConfig {
			if prof.PluginConfig[k].Name == noderesources.Name {
				// 更新已有插件参数，避免重复添加配置。
				pc := &prof.PluginConfig[k]
				args, ok := pc.Args.(*schedulerapi.NodeResourcesFitArgs)
				if !ok {
					return nil, fmt.Errorf("want args to be of type NodeResourcesFitArgs, got %T", pc.Args)
				}
				args.IgnoredResources = ignoredExtendedResources
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("can't find NodeResourcesFitArgs in plugin config")
		}
	}
	return fExtenders, nil
}

// FailureHandlerFn 定义调度失败时的处理函数签名。
type FailureHandlerFn func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *fwk.Status, nominatingInfo *fwk.NominatingInfo, start time.Time)

// unionedGVKs 把所有 profile 关注的事件资源及其动作类型取并集。
func unionedGVKs(queueingHintsPerProfile internalqueue.QueueingHintMapPerProfile) map[fwk.EventResource]fwk.ActionType {
	gvkMap := make(map[fwk.EventResource]fwk.ActionType)
	for _, queueingHints := range queueingHintsPerProfile {
		for evt := range queueingHints {
			if _, ok := gvkMap[evt.Resource]; ok {
				gvkMap[evt.Resource] |= evt.ActionType
			} else {
				gvkMap[evt.Resource] = evt.ActionType
			}
		}
	}
	return gvkMap
}

// newPodInformer 创建只返回非终态 Pod 的共享索引 informer。
// 该 informer 允许添加索引器，但只允许添加不冲突的索引器。
func newPodInformer(cs clientset.Interface, resyncPeriod time.Duration, informerName *cache.InformerName) cache.SharedIndexInformer {
	selector := fmt.Sprintf("status.phase!=%v,status.phase!=%v", v1.PodSucceeded, v1.PodFailed)
	tweakListOptions := func(options *metav1.ListOptions) {
		options.FieldSelector = selector
	}
	informer := coreinformers.NewPodInformerWithOptions(cs, metav1.NamespaceAll, internalinterfaces.InformerOptions{
		ResyncPeriod:     resyncPeriod,
		Indexers:         cache.Indexers{},
		TweakListOptions: tweakListOptions,
		InformerName:     informerName,
	})

	// 丢弃 `.metadata.managedFields` 以降低内存占用。
	// Extract 流程（即 `ExtractPod`）应当不会被用到。
	trim := func(obj interface{}) (interface{}, error) {
		if accessor, err := meta.Accessor(obj); err == nil {
			if accessor.GetManagedFields() != nil {
				accessor.SetManagedFields(nil)
			}
		}
		return obj, nil
	}
	informer.SetTransform(trim)
	return informer
}

// CurrentCycle 返回当前调度周期编号；队列未初始化时返回 0。
func (sched *Scheduler) CurrentCycle() int64 {
	if sched.SchedulingQueue != nil {
		return sched.SchedulingQueue.SchedulingCycle()
	}
	return 0
}
