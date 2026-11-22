/*
Copyright 2019 The Kubernetes Authors.

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

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	apidispatcher "k8s.io/kubernetes/pkg/scheduler/backend/api_dispatcher"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	// Permit 插件可返回的最大超时时间。
	maxTimeout = 15 * time.Minute
)

// frameworkImpl 负责初始化并运行调度器插件。
type frameworkImpl struct {
	registry                  Registry
	snapshotSharedLister      fwk.SharedLister
	mutableSnapshotLister     fwk.MutableSnapshotSharedLister
	waitingPods               *waitingPodsMap
	podsInPreBind             *podsInPreBindMap
	scorePluginWeight         map[string]int
	preEnqueuePlugins         []fwk.PreEnqueuePlugin
	enqueueExtensions         []fwk.EnqueueExtensions
	queueSortPlugins          []fwk.QueueSortPlugin
	preFilterPlugins          []fwk.PreFilterPlugin
	filterPlugins             []fwk.FilterPlugin
	postFilterPlugins         []fwk.PostFilterPlugin
	preScorePlugins           []fwk.PreScorePlugin
	scorePlugins              []fwk.ScorePlugin
	reservePlugins            []fwk.ReservePlugin
	preBindPlugins            []fwk.PreBindPlugin
	bindPlugins               []fwk.BindPlugin
	postBindPlugins           []fwk.PostBindPlugin
	permitPlugins             []fwk.PermitPlugin
	batchablePlugins          []fwk.SignPlugin
	podGroupPostFilterPlugins []fwk.PodGroupPostFilterPlugin

	placementGeneratePlugins   []fwk.PlacementGeneratePlugin
	placementFeasiblePlugins   []framework.PlacementFeasiblePlugin
	placementScorePlugins      []fwk.PlacementScorePlugin
	placementScorePluginWeight map[string]int

	// pluginsMap 按名字保存所有插件。
	pluginsMap map[string]fwk.Plugin

	clientSet        clientset.Interface
	kubeConfig       *restclient.Config
	eventRecorder    events.EventRecorderLogger
	informerFactory  informers.SharedInformerFactory
	sharedDRAManager fwk.SharedDRAManager
	podGroupManager  fwk.PodGroupManager
	logger           klog.Logger

	sharedCSIManager fwk.CSIManager

	metricsRecorder          *metrics.MetricAsyncRecorder
	profileName              string
	percentageOfNodesToScore *int32

	extenders []fwk.Extender
	fwk.PodNominator
	fwk.PodActivator
	apiDispatcher *apidispatcher.APIDispatcher
	apiCacher     fwk.APICacher

	parallelizer fwk.Parallelizer

	batch *OpportunisticBatch

	enableSignatures bool
}

// extensionPoint 封装某个扩展点上期望配置与实际应用的插件集合，
// 便于统一遍历 frameworkImpl 支持的所有扩展点。
type extensionPoint struct {
	// 该扩展点上期望配置的插件集合。
	plugins *config.PluginSet
	// 指向实际在该扩展点运行的插件切片的指针。

	slicePtr interface{}
}

func (f *frameworkImpl) getExtensionPoints(plugins *config.Plugins) []extensionPoint {
	return []extensionPoint{
		{&plugins.PreFilter, &f.preFilterPlugins},
		{&plugins.Filter, &f.filterPlugins},
		{&plugins.PostFilter, &f.postFilterPlugins},
		{&plugins.Reserve, &f.reservePlugins},
		{&plugins.PreScore, &f.preScorePlugins},
		{&plugins.Score, &f.scorePlugins},
		{&plugins.PreBind, &f.preBindPlugins},
		{&plugins.Bind, &f.bindPlugins},
		{&plugins.PostBind, &f.postBindPlugins},
		{&plugins.Permit, &f.permitPlugins},
		{&plugins.PreEnqueue, &f.preEnqueuePlugins},
		{&plugins.QueueSort, &f.queueSortPlugins},
		{&plugins.PlacementGenerate, &f.placementGeneratePlugins},
		{&plugins.PlacementScore, &f.placementScorePlugins},
		{&plugins.PodGroupPostFilter, &f.podGroupPostFilterPlugins},
	}
}

// Extenders 返回已注册的 extender。
func (f *frameworkImpl) Extenders() []fwk.Extender {
	return f.extenders
}

type frameworkOptions struct {
	componentConfigVersion string
	clientSet              clientset.Interface
	kubeConfig             *restclient.Config
	eventRecorder          events.EventRecorderLogger
	informerFactory        informers.SharedInformerFactory
	sharedDRAManager       fwk.SharedDRAManager
	sharedCSIManager       fwk.CSIManager
	snapshotSharedLister   fwk.SharedLister
	mutableSnapshotLister  fwk.MutableSnapshotSharedLister
	metricsRecorder        *metrics.MetricAsyncRecorder
	podNominator           fwk.PodNominator
	podActivator           fwk.PodActivator
	extenders              []fwk.Extender
	captureProfile         CaptureProfile
	parallelizer           parallelize.Parallelizer
	waitingPods            *waitingPodsMap
	podsInPreBind          *podsInPreBindMap
	apiDispatcher          *apidispatcher.APIDispatcher
	podGroupManager        fwk.PodGroupManager
	logger                 *klog.Logger
}

// Option 用于配置 frameworkImpl。
type Option func(*frameworkOptions)

// WithComponentConfigVersion 设置所使用的 KubeSchedulerConfiguration 版本。
// 参数应为转换来源外部类型的完整 scheme group/version（例如
// "kubescheduler.config.k8s.io/v1"）。
func WithComponentConfigVersion(componentConfigVersion string) Option {
	return func(o *frameworkOptions) {
		o.componentConfigVersion = componentConfigVersion
	}
}

// WithClientSet 为 frameworkImpl 设置 clientSet。
func WithClientSet(clientSet clientset.Interface) Option {
	return func(o *frameworkOptions) {
		o.clientSet = clientSet
	}
}

// WithKubeConfig 为 frameworkImpl 设置 kubeConfig。
func WithKubeConfig(kubeConfig *restclient.Config) Option {
	return func(o *frameworkOptions) {
		o.kubeConfig = kubeConfig
	}
}

// WithEventRecorder 为 frameworkImpl 设置事件记录器。
func WithEventRecorder(recorder events.EventRecorderLogger) Option {
	return func(o *frameworkOptions) {
		o.eventRecorder = recorder
	}
}

// WithInformerFactory 为 frameworkImpl 设置 informer 工厂。
func WithInformerFactory(informerFactory informers.SharedInformerFactory) Option {
	return func(o *frameworkOptions) {
		o.informerFactory = informerFactory
	}
}

// WithSharedDRAManager 为 framework 设置 SharedDRAManager。
func WithSharedDRAManager(sharedDRAManager fwk.SharedDRAManager) Option {
	return func(o *frameworkOptions) {
		o.sharedDRAManager = sharedDRAManager
	}
}

// WithSharedCSIManager 为 framework 设置 SharedCSIManager。
func WithSharedCSIManager(sharedCSIManager fwk.CSIManager) Option {
	return func(o *frameworkOptions) {
		o.sharedCSIManager = sharedCSIManager
	}
}

// WithSnapshotSharedLister 设置快照的 SharedLister。
func WithSnapshotSharedLister(snapshotSharedLister fwk.SharedLister) Option {
	return func(o *frameworkOptions) {
		o.snapshotSharedLister = snapshotSharedLister
	}
}

// WithMutableSnapshotLister 设置 MutableSnapshotLister。
func WithMutableSnapshotLister(mutableSnapshotLister fwk.MutableSnapshotSharedLister) Option {
	return func(o *frameworkOptions) {
		o.mutableSnapshotLister = mutableSnapshotLister
	}
}

// WithPodNominator 为 frameworkImpl 设置 podNominator。
func WithPodNominator(nominator fwk.PodNominator) Option {
	return func(o *frameworkOptions) {
		o.podNominator = nominator
	}
}

// WithPodActivator 为 frameworkImpl 设置 podActivator。
func WithPodActivator(activator fwk.PodActivator) Option {
	return func(o *frameworkOptions) {
		o.podActivator = activator
	}
}

// WithExtenders 为 frameworkImpl 设置 extenders。
func WithExtenders(extenders []fwk.Extender) Option {
	return func(o *frameworkOptions) {
		o.extenders = extenders
	}
}

// WithParallelism 设置 frameworkImpl 的并行度。
func WithParallelism(parallelism int) Option {
	return func(o *frameworkOptions) {
		o.parallelizer = parallelize.NewParallelizer(parallelism)
	}
}

// WithAPIDispatcher 为 frameworkImpl 设置 API dispatcher。
func WithAPIDispatcher(apiDispatcher *apidispatcher.APIDispatcher) Option {
	return func(o *frameworkOptions) {
		o.apiDispatcher = apiDispatcher
	}
}

// WithPodGroupManager 为 frameworkImpl 设置 PodGroupManager。
func WithPodGroupManager(podGroupManager fwk.PodGroupManager) Option {
	return func(o *frameworkOptions) {
		o.podGroupManager = podGroupManager
	}
}

// CaptureProfile 是用于捕获最终配置的回调。
type CaptureProfile func(config.KubeSchedulerProfile)

// WithCaptureProfile 设置捕获最终配置的回调。
func WithCaptureProfile(c CaptureProfile) Option {
	return func(o *frameworkOptions) {
		o.captureProfile = c
	}
}

// WithMetricsRecorder 为 frameworkImpl 设置指标记录器。
func WithMetricsRecorder(r *metrics.MetricAsyncRecorder) Option {
	return func(o *frameworkOptions) {
		o.metricsRecorder = r
	}
}

// WithWaitingPods 为 frameworkImpl 设置等待中的 Pod 集合。
func WithWaitingPods(wp *waitingPodsMap) Option {
	return func(o *frameworkOptions) {
		o.waitingPods = wp
	}
}

// WithPodsInPreBind 为 frameworkImpl 设置处于 preBind 阶段的 Pod 集合。
func WithPodsInPreBind(bp *podsInPreBindMap) Option {
	return func(o *frameworkOptions) {
		o.podsInPreBind = bp
	}
}

// WithLogger 覆盖 k8s.io/klog 的默认 logger。
func WithLogger(logger klog.Logger) Option {
	return func(o *frameworkOptions) {
		o.logger = &logger
	}
}

// defaultFrameworkOptions 为未被 Option 覆盖的字段提供默认值。
func defaultFrameworkOptions(stopCh <-chan struct{}) frameworkOptions {
	return frameworkOptions{
		metricsRecorder: metrics.NewMetricsAsyncRecorder(1000, time.Second, stopCh),
		parallelizer:    parallelize.NewParallelizer(parallelize.DefaultParallelism),
	}
}

var _ framework.Framework = &frameworkImpl{}

// NewFramework 根据配置与注册表初始化插件。
//
// 它会创建后台 goroutine（例如通过 defaultFrameworkOptions -> metrics.NewMetricsAsyncRecorder），
// 这些 goroutine 会一直运行到 context 被取消。可用 WaitForShutdown 阻塞等待它们结束。
func NewFramework(ctx context.Context, r Registry, profile *config.KubeSchedulerProfile, opts ...Option) (framework.Framework, error) {
	options := defaultFrameworkOptions(ctx.Done())
	for _, opt := range opts {
		opt(&options)
	}

	logger := klog.FromContext(ctx)
	if options.logger != nil {
		logger = *options.logger
	}
	f := &frameworkImpl{
		registry:              r,
		snapshotSharedLister:  options.snapshotSharedLister,
		mutableSnapshotLister: options.mutableSnapshotLister,
		sharedCSIManager:      options.sharedCSIManager,
		waitingPods:           options.waitingPods,
		podsInPreBind:         options.podsInPreBind,
		clientSet:             options.clientSet,
		kubeConfig:            options.kubeConfig,
		eventRecorder:         options.eventRecorder,
		informerFactory:       options.informerFactory,
		sharedDRAManager:      options.sharedDRAManager,
		metricsRecorder:       options.metricsRecorder,
		extenders:             options.extenders,
		PodNominator:          options.podNominator,
		PodActivator:          options.podActivator,
		apiDispatcher:         options.apiDispatcher,
		podGroupManager:       options.podGroupManager,
		parallelizer:          options.parallelizer,
		logger:                logger,
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
		f.batch = newOpportunisticBatch(f, utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload))
	}

	if len(f.extenders) > 0 {
		// Extender 不支持调度框架中 EnqueueExtensions 那样的重新入队机制。
		// 这里为 fwk.ExtenderName 注册一个默认实现；当调度周期中 Extender 拒绝某些节点导致 Pod
		// 不可调度时，就把 fwk.ExtenderName 记入 pInfo.UnschedulablePlugins。

		f.enqueueExtensions = []fwk.EnqueueExtensions{&defaultEnqueueExtension{pluginName: framework.ExtenderName}}
	}

	if profile == nil {
		return f, nil
	}

	f.profileName = profile.SchedulerName
	f.percentageOfNodesToScore = profile.PercentageOfNodesToScore
	if profile.Plugins == nil {
		return f, nil
	}

	// 从配置中找出所需插件。
	pg := f.pluginsNeeded(profile.Plugins)

	pluginConfig := make(map[string]runtime.Object, len(profile.PluginConfig))
	for i := range profile.PluginConfig {
		name := profile.PluginConfig[i].Name
		if _, ok := pluginConfig[name]; ok {
			return nil, fmt.Errorf("repeated config for plugin %s", name)
		}
		pluginConfig[name] = profile.PluginConfig[i].Args
	}
	outputProfile := config.KubeSchedulerProfile{
		SchedulerName:            f.profileName,
		PercentageOfNodesToScore: f.percentageOfNodesToScore,
		Plugins:                  profile.Plugins,
		PluginConfig:             make([]config.PluginConfig, 0, len(pg)),
	}

	f.pluginsMap = make(map[string]fwk.Plugin)
	for name, factory := range r {
		// 只初始化需要的插件。
		if !pg.Has(name) {
			continue
		}

		args := pluginConfig[name]
		if args != nil {
			outputProfile.PluginConfig = append(outputProfile.PluginConfig, config.PluginConfig{
				Name: name,
				Args: args,
			})
		}
		p, err := factory(ctx, args, f)
		if err != nil {
			return nil, fmt.Errorf("initializing plugin %q: %w", name, err)
		}
		f.pluginsMap[name] = p

		f.fillEnqueueExtensions(p)
	}

	// 按各扩展点初始化插件列表。
	for _, e := range f.getExtensionPoints(profile.Plugins) {
		if err := updatePluginList(e.slicePtr, *e.plugins, f.pluginsMap); err != nil {
			return nil, err
		}
	}

	// 将 MultiPoint 插件展开到各自对应的扩展点。
	if len(profile.Plugins.MultiPoint.Enabled) > 0 {
		if err := f.expandMultiPointPlugins(logger, profile); err != nil {
			return nil, err
		}
	}

	if len(f.queueSortPlugins) != 1 {
		return nil, fmt.Errorf("only one queue sort plugin required for profile with scheduler name %q, but got %d", profile.SchedulerName, len(f.queueSortPlugins))
	}
	if len(f.bindPlugins) == 0 {
		return nil, fmt.Errorf("at least one bind plugin is needed for profile with scheduler name %q", profile.SchedulerName)
	}
	if len(f.placementGeneratePlugins) > 1 {
		return nil, fmt.Errorf("at most one placement generate plugin is allowed for profile with scheduler name %q", profile.SchedulerName)
	}

	podScoreWeights, err := getValidScoreWeights(f, reflect.TypeFor[fwk.ScorePlugin](), append(profile.Plugins.Score.Enabled, profile.Plugins.MultiPoint.Enabled...))
	if err != nil {
		return nil, fmt.Errorf("score plugins: %w", err)
	}
	f.scorePluginWeight = podScoreWeights

	// 再次校验 Score 权重：Plugin.Name() 返回值可能与配置中使用的名字不同。

	for _, scorePlugin := range f.scorePlugins {
		if f.scorePluginWeight[scorePlugin.Name()] == 0 {
			return nil, fmt.Errorf("score plugin %q is not configured with weight", scorePlugin.Name())
		}
	}

	placementScoreWeights, err := getValidScoreWeights(f, reflect.TypeFor[fwk.PlacementScorePlugin](), append(profile.Plugins.PlacementScore.Enabled, profile.Plugins.MultiPoint.Enabled...))
	if err != nil {
		return nil, fmt.Errorf("placement score plugins: %w", err)
	}
	f.placementScorePluginWeight = placementScoreWeights

	// 再次校验 PlacementScore 权重：Plugin.Name() 返回值可能与配置中使用的名字不同。

	for _, placementScorePlugin := range f.placementScorePlugins {
		if f.placementScorePluginWeight[placementScorePlugin.Name()] == 0 {
			return nil, fmt.Errorf("placement score plugin %q is not configured with weight", placementScorePlugin.Name())
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
		f.computeBatchablePlugins()
	}

	// 使用 GangScheduling 插件作为唯一的 PlacementFeasiblePlugin。
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		if gs, ok := f.pluginsMap[names.GangScheduling]; ok {
			if p, ok := gs.(framework.PlacementFeasiblePlugin); ok {
				f.placementFeasiblePlugins = append(f.placementFeasiblePlugins, p)
			} else {
				return nil, fmt.Errorf("GenericWorkload is enabled, but GangScheduling plugin does not fulfill PlacementFeasiblePlugin interface")
			}
		} else {
			logger.Error(nil, "GenericWorkload is enabled, but GangScheduling plugin is not set. Gang scheduling using PodGroups may not work as expected.")
		}
	}

	if options.captureProfile != nil {
		if len(outputProfile.PluginConfig) != 0 {
			sort.Slice(outputProfile.PluginConfig, func(i, j int) bool {
				return outputProfile.PluginConfig[i].Name < outputProfile.PluginConfig[j].Name
			})
		} else {
			outputProfile.PluginConfig = nil
		}
		options.captureProfile(outputProfile)
	}

	// 记录各扩展点启用的插件，综合默认插件、用户配置与 MultiPoint 展开结果。
	logger.V(2).Info("the scheduler starts to work with those plugins", "Plugins", *f.ListPlugins())
	f.setInstrumentedPlugins()
	return f, nil
}

// WaitForShutdown 等待 NewFramework 创建的所有后台 goroutine 结束。
// 必须取消传给 NewFramework 的 context，这些 goroutine 才会停止。
func WaitForShutdown(f framework.Framework) {
	if f.(*frameworkImpl).metricsRecorder != nil {
		<-f.(*frameworkImpl).metricsRecorder.IsStoppedCh
	}
}

// setInstrumentedPlugins 用 frameworkImpl 当前的插件创建带指标采集的包装插件。
func (f *frameworkImpl) setInstrumentedPlugins() {
	// 缓存 PreFilter 与 Filter 插件的指标向量，避免每次调用查表。
	for i, pl := range f.preFilterPlugins {
		f.preFilterPlugins[i] = &instrumentedPreFilterPlugin{
			PreFilterPlugin: f.preFilterPlugins[i],
			metric:          metrics.PluginEvaluationTotal.WithLabelValues(pl.Name(), metrics.PreFilter, f.profileName),
		}
	}
	for i, pl := range f.filterPlugins {
		f.filterPlugins[i] = &instrumentedFilterPlugin{
			FilterPlugin: f.filterPlugins[i],
			metric:       metrics.PluginEvaluationTotal.WithLabelValues(pl.Name(), metrics.Filter, f.profileName),
		}
	}

	// 缓存 PreScore 与 Score 插件的指标向量。
	for i, pl := range f.preScorePlugins {
		f.preScorePlugins[i] = &instrumentedPreScorePlugin{
			PreScorePlugin: f.preScorePlugins[i],
			metric:         metrics.PluginEvaluationTotal.WithLabelValues(pl.Name(), metrics.PreScore, f.profileName),
		}
	}
	for i, pl := range f.scorePlugins {
		f.scorePlugins[i] = &instrumentedScorePlugin{
			ScorePlugin: f.scorePlugins[i],
			metric:      metrics.PluginEvaluationTotal.WithLabelValues(pl.Name(), metrics.Score, f.profileName),
		}
	}
}

// SetPodNominator 替换 framework 使用的 PodNominator。
func (f *frameworkImpl) SetPodNominator(n fwk.PodNominator) {
	f.PodNominator = n
}

// SetPodActivator 替换 framework 使用的 PodActivator。
func (f *frameworkImpl) SetPodActivator(a fwk.PodActivator) {
	f.PodActivator = a
}

// SetAPICacher 设置 framework 使用的 APICacher。
func (f *frameworkImpl) SetAPICacher(c fwk.APICacher) {
	f.apiCacher = c
}

// Close 依次关闭实现了 io.Closer 的插件。
func (f *frameworkImpl) Close() error {
	var errs []error
	for name, plugin := range f.pluginsMap {
		if closer, ok := plugin.(io.Closer); ok {
			err := closer.Close()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s failed to close: %w", name, err))
				// 即使部分插件关闭失败，也要尝试关闭其余插件。
			}
		}
	}
	return errors.Join(errs...)
}

// getValidScoreWeights 确保 MultiPoint-Score 与单独 Score 插件的权重之和不会让 MaxTotalScore 溢出。
// pluginType 指允许定义权重的插件类型，如 Score 或 PlacementScore。
func getValidScoreWeights(f *frameworkImpl, pluginType reflect.Type, plugins []config.Plugin) (map[string]int, error) {
	weights := make(map[string]int)
	var totalPriority int64
	for _, e := range plugins {
		pg := f.pluginsMap[e.Name]
		if !reflect.TypeOf(pg).Implements(pluginType) {
			continue
		}

		// MultiPoint 插件被追加到 Score 插件列表中；若某插件已出现过，以单独配置的 Score 权重为准。

		if _, ok := weights[e.Name]; ok {
			continue
		}
		// 权重不能为零；如需禁用插件，应在配置中显式禁用。

		weights[e.Name] = int(e.Weight)
		if weights[e.Name] == 0 {
			weights[e.Name] = 1
		}

		// 校验 totalPriority 防止超过 MaxTotalScore 而溢出。
		if int64(weights[e.Name])*fwk.MaxScore > fwk.MaxTotalScore-totalPriority {
			return nil, fmt.Errorf("total score of Score plugins could overflow")
		}
		totalPriority += int64(weights[e.Name]) * fwk.MaxScore
	}
	return weights, nil
}

type orderedSet struct {
	set         map[string]int
	list        []string
	deletionCnt int
}

func newOrderedSet() *orderedSet {
	return &orderedSet{set: make(map[string]int)}
}

func (os *orderedSet) insert(s string) {
	if os.has(s) {
		return
	}
	os.set[s] = len(os.list)
	os.list = append(os.list, s)
}

func (os *orderedSet) has(s string) bool {
	_, found := os.set[s]
	return found
}

func (os *orderedSet) delete(s string) {
	if i, found := os.set[s]; found {
		delete(os.set, s)
		os.list = append(os.list[:i-os.deletionCnt], os.list[i+1-os.deletionCnt:]...)
		os.deletionCnt++
	}
}

func (f *frameworkImpl) expandMultiPointPlugins(logger klog.Logger, profile *config.KubeSchedulerProfile) error {
	// 初始化 MultiPoint 插件。
	for _, e := range f.getExtensionPoints(profile.Plugins) {
		plugins := reflect.ValueOf(e.slicePtr).Elem()
		pluginType := plugins.Type().Elem()
		// 记录在普通扩展点已启用的插件，用于检测重复注册。

		enabledSet := newOrderedSet()
		for _, plugin := range e.plugins.Enabled {
			enabledSet.insert(plugin.Name)
		}

		disabledSet := sets.New[string]()
		for _, disabledPlugin := range e.plugins.Disabled {
			disabledSet.Insert(disabledPlugin.Name)
		}
		if disabledSet.Has("*") {
			logger.V(4).Info("Skipped MultiPoint expansion because all plugins are disabled for extension point", "extension", pluginType)
			continue
		}

		// 把通过 MultiPoint 启用的插件与通过具体扩展点启用的分开记录，
		// 以便区分重复注册和显式覆盖。
		multiPointEnabled := newOrderedSet()
		overridePlugins := newOrderedSet()
		for _, ep := range profile.Plugins.MultiPoint.Enabled {
			pg, ok := f.pluginsMap[ep.Name]
			if !ok {
				return fmt.Errorf("%s %q does not exist", pluginType.Name(), ep.Name)
			}

			// 若该插件未实现当前待展开扩展点所需的类型，跳过。
			if !reflect.TypeOf(pg).Implements(pluginType) {
				continue
			}

			// MultiPoint 启用的插件仍可在具体扩展点被禁用。
			if disabledSet.Has(ep.Name) {
				logger.V(4).Info("Skipped disabled plugin for extension point", "plugin", ep.Name, "extension", pluginType)
				continue
			}

			// 若该插件已由具体扩展点启用，说明用户意在覆盖默认插件或做了其他显式设置，
			// 此时以该设置为准，丢弃 MultiPoint 的值。
			// 这样可保持覆盖默认插件的既有行为（见 https://github.com/kubernetes/kubernetes/pull/99582）。

			if enabledSet.has(ep.Name) {
				overridePlugins.insert(ep.Name)
				logger.Info("MultiPoint plugin is explicitly re-configured; overriding", "plugin", ep.Name)
				continue
			}

			// 若该插件已通过 MultiPoint 注册过，则属于重复注册，是配置错误。

			if multiPointEnabled.has(ep.Name) {
				return fmt.Errorf("plugin %q already registered as %q", ep.Name, pluginType.Name())
			}

			// 此处只需更新 multipoint 集合，具体扩展点的集合上面已获取。
			multiPointEnabled.insert(ep.Name)
		}

		// 重排插件，期望顺序为：
		// - 第一部分：overridePlugins，保持其在普通扩展点中的顺序；
		// - 第二部分：multiPointEnabled，即只在 MultiPoint 中定义的插件；
		// - 第三部分：普通扩展点中其余插件（不在前两部分内）。
		newPlugins := reflect.New(reflect.TypeOf(e.slicePtr).Elem()).Elem()
		// 第一部分
		for _, name := range slices.Clone(enabledSet.list) {
			if overridePlugins.has(name) {
				newPlugins = reflect.Append(newPlugins, reflect.ValueOf(f.pluginsMap[name]))
				enabledSet.delete(name)
			}
		}
		// 第二部分
		for _, name := range multiPointEnabled.list {
			newPlugins = reflect.Append(newPlugins, reflect.ValueOf(f.pluginsMap[name]))
		}
		// 第三部分
		for _, name := range enabledSet.list {
			newPlugins = reflect.Append(newPlugins, reflect.ValueOf(f.pluginsMap[name]))
		}
		plugins.Set(newPlugins)
	}
	return nil
}

func shouldHaveEnqueueExtensions(p fwk.Plugin) bool {
	switch p.(type) {
	// 只有 PreEnqueue、PreFilter、Filter、Reserve 和 Permit 插件可以（也应该）有 EnqueueExtensions，
	// 详细原因参见 EnqueueExtensions 的注释。
	case fwk.PreEnqueuePlugin, fwk.PreFilterPlugin, fwk.FilterPlugin, fwk.ReservePlugin, fwk.PermitPlugin:
		return true
	}
	return false
}

func (f *frameworkImpl) fillEnqueueExtensions(p fwk.Plugin) {
	if !shouldHaveEnqueueExtensions(p) {
		// 忽略其他类型插件上的 EnqueueExtensions。
		return
	}

	ext, ok := p.(fwk.EnqueueExtensions)
	if !ok {
		// 插件未实现 EnqueueExtensions 时，为其注册默认实现；
		// 因为我们无从得知它关心哪些事件，这样也能保持向后兼容。

		f.enqueueExtensions = append(f.enqueueExtensions, &defaultEnqueueExtension{pluginName: p.Name()})
		return
	}

	f.enqueueExtensions = append(f.enqueueExtensions, ext)
}

// defaultEnqueueExtension 用于插件未实现 EnqueueExtensions 接口的情形。
type defaultEnqueueExtension struct {
	pluginName string
}

// Name 返回该默认扩展所代理的插件名。
func (p *defaultEnqueueExtension) Name() string { return p.pluginName }

// EventsToRegister 展开通配资源，返回所有具体集群事件以注册事件处理器。
func (p *defaultEnqueueExtension) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	// 必须返回带 framework.All 动作的所有具体集群事件，而非通配符事件，
	// 因为返回值将被用于注册事件处理器；若返回通配符，插件自己注册的事件处理器不受影响，
	// 但可能导致部分事件未被注册。

	return framework.UnrollWildCardResource(), nil
}

func updatePluginList(pluginList interface{}, pluginSet config.PluginSet, pluginsMap map[string]fwk.Plugin) error {
	plugins := reflect.ValueOf(pluginList).Elem()
	pluginType := plugins.Type().Elem()
	set := sets.New[string]()
	for _, ep := range pluginSet.Enabled {
		pg, ok := pluginsMap[ep.Name]
		if !ok {
			return fmt.Errorf("%s %q does not exist", pluginType.Name(), ep.Name)
		}

		if !reflect.TypeOf(pg).Implements(pluginType) {
			return fmt.Errorf("plugin %q does not extend %s plugin", ep.Name, pluginType.Name())
		}

		if set.Has(ep.Name) {
			return fmt.Errorf("plugin %q already registered as %q", ep.Name, pluginType.Name())
		}

		set.Insert(ep.Name)

		newPlugins := reflect.Append(plugins, reflect.ValueOf(pg))
		plugins.Set(newPlugins)
	}
	return nil
}

// PreEnqueuePlugins 返回已注册的 PreEnqueue 插件。
func (f *frameworkImpl) PreEnqueuePlugins() []fwk.PreEnqueuePlugin {
	return f.preEnqueuePlugins
}

// EnqueueExtensions 返回已注册的重入队扩展。
func (f *frameworkImpl) EnqueueExtensions() []fwk.EnqueueExtensions {
	return f.enqueueExtensions
}

// QueueSortFunc 返回用于调度队列排序的比较函数。
func (f *frameworkImpl) QueueSortFunc() fwk.LessFunc {
	if f == nil {
		// frameworkImpl 为 nil 时保持原顺序不变。
		// 注意：这主要用于测试。
		return func(_, _ fwk.QueuedEntityInfo) bool { return false }
	}

	if len(f.queueSortPlugins) == 0 {
		panic("No QueueSort plugin is registered in the frameworkImpl.")
	}

	// 只能启用一个 QueueSort 插件。
	return f.queueSortPlugins[0].Less
}

// 只要 PreFilter、Filter、PreScore 或 Score 插件中有未实现签名机制的，就禁用缓存。
func (f *frameworkImpl) computeBatchablePlugins() {
	f.enableSignatures = true

	if len(f.extenders) > 0 {
		f.logger.Info("Disabling signatures for profile because it has extenders configured.",
			"profile", f.profileName)

		f.enableSignatures = false
	}

	// 收集所有兼容类型的插件。
	candidatePlugins := []fwk.Plugin{}
	for _, pl := range f.preFilterPlugins {
		candidatePlugins = append(candidatePlugins, pl)
	}
	for _, pl := range f.filterPlugins {
		candidatePlugins = append(candidatePlugins, pl)
	}
	for _, pl := range f.preScorePlugins {
		candidatePlugins = append(candidatePlugins, pl)
	}
	for _, pl := range f.scorePlugins {
		candidatePlugins = append(candidatePlugins, pl)
	}

	// 从插件中提取签名要素。
	plugins := map[string]fwk.SignPlugin{}
	unsupportedPlugins := sets.New[string]()
	for _, pl := range candidatePlugins {
		if _, found := plugins[pl.Name()]; !found {
			if _, implements := pl.(fwk.SignPlugin); implements {
				f.batchablePlugins = append(f.batchablePlugins, pl.(fwk.SignPlugin))
				plugins[pl.Name()] = pl.(fwk.SignPlugin)
			} else {
				unsupportedPlugins.Insert(pl.Name())
				f.enableSignatures = false
			}
		}
	}

	if !f.enableSignatures {
		f.logger.Info("Disabling signatures for profile because plugins do not support it.",
			"profile", f.profileName, "plugins", unsupportedPlugins.UnsortedList())
	}
}

// SignPod 为给定 Pod 生成签名。相同签名的两个 Pod 在相同状态下对同一组节点应得到
// 相同的可行性与评分。若有插件无法为 Pod 构造签名，则返回 nil，表示无法与其他 Pod 比较，
// 该 Pod 上的若干优化会被关闭。
func (f *frameworkImpl) SignPod(ctx context.Context, pod *v1.Pod) fwk.PodSignature {
	logger := klog.FromContext(ctx)
	var status *fwk.Status

	startTime := time.Now()
	defer func() {
		f.metricsRecorder.ObserveFrameworkExtensionPointDurationAsync(metrics.Sign, status.Code().String(), f.profileName, metrics.SinceInSeconds(startTime))
	}()

	if !f.enableSignatures {
		return nil
	}

	sig := map[string]any{
		fwk.SchedulerNameSignerName: pod.Spec.SchedulerName,
	}

	for _, plugin := range f.batchablePlugins {
		startTime := time.Now()
		fragments, status := plugin.SignPod(ctx, pod)
		f.metricsRecorder.ObservePluginDurationAsync(metrics.Sign, plugin.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))

		if !status.IsSuccess() {
			if status.Code() == fwk.Error {
				logger.Error(status.AsError(), "SignPod failed for plugin", "plugin", plugin.Name())
			}
			logger.V(5).Info("SignPod can't sign pod due to plugin", "plugin", plugin.Name(), "status", status)
			return nil
		}

		for _, elem := range fragments {
			sig[elem.Key] = elem.Value
		}
	}

	sigBytes, err := json.Marshal(sig)
	if err != nil {
		logger.Error(err, "SignPod failed to marshal signature object")
		return nil
	}

	return sigBytes
}

// RunPreFilterPlugins 依次运行配置的 PreFilter 插件。任一插件返回 Success/Skip 之外的状态时，
// 返回的状态码即为非成功。
// 返回 Skip 时，PreFilterResult 及状态中的其他字段会被忽略，对应的 Filter 插件与
// PreFilterExtensions() 在本调度周期中也不会执行。
// 返回非成功状态时，调度周期中止。
func (f *frameworkImpl) RunPreFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod) (_ *fwk.PreFilterResult, status *fwk.Status, _ sets.Set[string]) {
	startTime := time.Now()
	skipPlugins := sets.New[string]()
	defer func() {
		state.SetSkipFilterPlugins(skipPlugins)
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PreFilter, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	nodes, err := f.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("getting all nodes: %w", err)), nil
	}
	var result *fwk.PreFilterResult
	pluginsWithNodes := sets.New[string]()
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PreFilter")
	}
	var returnStatus *fwk.Status
	for _, pl := range f.preFilterPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		r, s := f.runPreFilterPlugin(ctx, pl, state, pod, nodes)
		if s.IsSkip() {
			skipPlugins.Insert(pl.Name())
			continue
		}
		if !s.IsSuccess() {
			s.SetPlugin(pl.Name())
			if s.Code() == fwk.UnschedulableAndUnresolvable {
				// 这种情况本调度周期不应发生抢占，因此无需执行剩余的 PreFilter。

				return nil, s, nil
			}
			if s.Code() == fwk.Unschedulable {
				// 这种情况抢占会在本调度周期稍后进行，因此需要执行完所有 PreFilter。
				// https://github.com/kubernetes/kubernetes/issues/119770

				returnStatus = s
				continue
			}
			return nil, fwk.AsStatus(fmt.Errorf("running PreFilter plugin %q: %w", pl.Name(), s.AsError())).WithPlugin(pl.Name()), nil
		}
		if !r.AllNodes() {
			pluginsWithNodes.Insert(pl.Name())
		}
		result = result.Merge(r)
		if !result.AllNodes() && len(result.NodeNames) == 0 {
			msg := fmt.Sprintf("node(s) didn't satisfy plugin(s) %v simultaneously", sets.List(pluginsWithNodes))
			if len(pluginsWithNodes) == 1 {
				msg = fmt.Sprintf("node(s) didn't satisfy plugin %v", sets.List(pluginsWithNodes)[0])
			}

			// PreFilterResult 过滤掉的节点会被框架视为 UnschedulableAndUnresolvable。
			return result, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, msg), pluginsWithNodes
		}
	}
	return result, returnStatus, pluginsWithNodes
}

func (f *frameworkImpl) runPreFilterPlugin(ctx context.Context, pl fwk.PreFilterPlugin, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PreFilter(ctx, state, pod, nodes)
	}
	startTime := time.Now()
	result, status := pl.PreFilter(ctx, state, pod, nodes)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PreFilter, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return result, status
}

// RunPreFilterExtensionAddPod 调用各 PreFilter 插件的 AddPod 接口，
// 任一插件返回非 Success 状态时立即返回。
func (f *frameworkImpl) RunPreFilterExtensionAddPod(
	ctx context.Context,
	state fwk.CycleState,
	podToSchedule *v1.Pod,
	podInfoToAdd fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
) (status *fwk.Status) {
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PreFilterExtension")
	}
	for _, pl := range f.preFilterPlugins {
		if pl.PreFilterExtensions() == nil || state.GetSkipFilterPlugins().Has(pl.Name()) {
			continue
		}
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		status = f.runPreFilterExtensionAddPod(ctx, pl, state, podToSchedule, podInfoToAdd, nodeInfo)
		if !status.IsSuccess() {
			err := status.AsError()
			logger.Error(err, "Plugin failed", "pod", klog.KObj(podToSchedule), "node", klog.KObj(nodeInfo.Node()), "operation", "addPod", "plugin", pl.Name())
			return fwk.AsStatus(fmt.Errorf("running AddPod on PreFilter plugin %q: %w", pl.Name(), err))
		}
	}

	return nil
}

func (f *frameworkImpl) runPreFilterExtensionAddPod(ctx context.Context, pl fwk.PreFilterPlugin, state fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PreFilterExtensions().AddPod(ctx, state, podToSchedule, podInfoToAdd, nodeInfo)
	}
	startTime := time.Now()
	status := pl.PreFilterExtensions().AddPod(ctx, state, podToSchedule, podInfoToAdd, nodeInfo)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PreFilterExtensionAddPod, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunPreFilterExtensionRemovePod 调用各 PreFilter 插件的 RemovePod 接口，
// 任一插件返回非 Success 状态时立即返回。
func (f *frameworkImpl) RunPreFilterExtensionRemovePod(
	ctx context.Context,
	state fwk.CycleState,
	podToSchedule *v1.Pod,
	podInfoToRemove fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
) (status *fwk.Status) {
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PreFilterExtension")
	}
	for _, pl := range f.preFilterPlugins {
		if pl.PreFilterExtensions() == nil || state.GetSkipFilterPlugins().Has(pl.Name()) {
			continue
		}
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		status = f.runPreFilterExtensionRemovePod(ctx, pl, state, podToSchedule, podInfoToRemove, nodeInfo)
		if !status.IsSuccess() {
			err := status.AsError()
			var node *v1.Node
			if nodeInfo != nil {
				node = nodeInfo.Node()
			}
			logger.Error(err, "Plugin failed", "node", klog.KObj(node), "operation", "removePod", "plugin", pl.Name(), "pod", klog.KObj(podToSchedule))
			return fwk.AsStatus(fmt.Errorf("running RemovePod on PreFilter plugin %q: %w", pl.Name(), err))
		}
	}

	return nil
}

func (f *frameworkImpl) runPreFilterExtensionRemovePod(ctx context.Context, pl fwk.PreFilterPlugin, state fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PreFilterExtensions().RemovePod(ctx, state, podToSchedule, podInfoToRemove, nodeInfo)
	}
	startTime := time.Now()
	status := pl.PreFilterExtensions().RemovePod(ctx, state, podToSchedule, podInfoToRemove, nodeInfo)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PreFilterExtensionRemovePod, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunFilterPlugins 在给定节点上运行配置的 Filter 插件。任一插件未返回 Success，
// 该节点即不适合运行此 Pod，同时会记录失败原因与状态。
func (f *frameworkImpl) RunFilterPlugins(
	ctx context.Context,
	state fwk.CycleState,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
) *fwk.Status {
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "Filter")
	}

	for _, pl := range f.filterPlugins {
		if state.GetSkipFilterPlugins().Has(pl.Name()) {
			continue
		}
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		if status := f.runFilterPlugin(ctx, pl, state, pod, nodeInfo); !status.IsSuccess() {
			if !status.IsRejected() {
				// Filter 插件不应返回 Success 或 Unschedulable 之外的状态。

				status = fwk.AsStatus(fmt.Errorf("running %q filter plugin: %w", pl.Name(), status.AsError()))
			}
			status.SetPlugin(pl.Name())
			return status
		}
	}

	return nil
}

func (f *frameworkImpl) runFilterPlugin(ctx context.Context, pl fwk.FilterPlugin, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.Filter(ctx, state, pod, nodeInfo)
	}
	startTime := time.Now()
	status := pl.Filter(ctx, state, pod, nodeInfo)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.Filter, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunPostFilterPlugins 依次运行 PostFilter 插件，遇到首个 Success、Error 或
// UnschedulableAndUnresolvable 即返回；否则执行完所有插件。
func (f *frameworkImpl) RunPostFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, filteredNodeStatusMap fwk.NodeToStatusReader) (_ *fwk.PostFilterResult, status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PostFilter, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()

	if state.ShouldSkipAllPostFilterPlugins() {
		return nil, fwk.NewStatus(fwk.Unschedulable, "All PostFilter plugins are skipped")
	}

	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PostFilter")
	}

	// result 记录最后一个有意义的（非 noop）PostFilterResult。
	var result *fwk.PostFilterResult
	var reasons []string
	var rejectorPlugin string
	for _, pl := range f.postFilterPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		r, s := f.runPostFilterPlugin(ctx, pl, state, pod, filteredNodeStatusMap)
		if s.IsSuccess() {
			return r, s
		} else if s.Code() == fwk.UnschedulableAndUnresolvable {
			return r, s.WithPlugin(pl.Name())
		} else if !s.IsRejected() {
			// Success、Unschedulable 与 UnschedulableAndUnresolvable 之外的状态均视为 Error。
			return nil, fwk.AsStatus(s.AsError()).WithPlugin(pl.Name())
		} else if r != nil && r.Mode() != fwk.ModeNoop {
			result = r
		}

		reasons = append(reasons, s.Reasons()...)
		// 记录首个失败的插件，除非后续证实另一个更相关。

		if len(rejectorPlugin) == 0 {
			rejectorPlugin = pl.Name()
		}
	}

	return result, fwk.NewStatus(fwk.Unschedulable, reasons...).WithPlugin(rejectorPlugin)
}

func (f *frameworkImpl) runPostFilterPlugin(ctx context.Context, pl fwk.PostFilterPlugin, state fwk.CycleState, pod *v1.Pod, filteredNodeStatusMap fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PostFilter(ctx, state, pod, filteredNodeStatusMap)
	}
	startTime := time.Now()
	r, s := pl.PostFilter(ctx, state, pod, filteredNodeStatusMap)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PostFilter, pl.Name(), s.Code().String(), metrics.SinceInSeconds(startTime))
	return r, s
}

// RunPodGroupPostFilterPlugins 依次运行配置的 PodGroupPostFilter 插件。
func (f *frameworkImpl) RunPodGroupPostFilterPlugins(ctx context.Context, state *framework.CycleState, podGroupInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (_ *fwk.PodGroupPostFilterResult, status *fwk.Status) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		return nil, fwk.NewStatus(fwk.Unschedulable, "generic workload feature is disabled, cannot perform PodGroupPostFilter")
	}

	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PodGroupPostFilter, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()

	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PodGroupPostFilter")
	}

	var reasons []string
	var rejectorPlugin string
	for _, pl := range f.podGroupPostFilterPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		res, status := f.runPodGroupPostFilterPlugin(ctx, pl, state, podGroupInfo, pgSchedulingFunc)
		if status.IsSuccess() {
			return res, status
		} else if status.Code() == fwk.UnschedulableAndUnresolvable {
			return res, status.WithPlugin(pl.Name())
		} else if status.Code() == fwk.Unschedulable {
			reasons = append(reasons, status.Reasons()...)
			if rejectorPlugin == "" {
				rejectorPlugin = pl.Name()
			}
		} else {
			// Success、Unschedulable 与 UnschedulableAndUnresolvable 之外的状态均视为 Error。
			return nil, fwk.AsStatus(fmt.Errorf("error in %q PodGroupPostFilter plugins: %s", pl.Name(), status.Message())).WithPlugin(pl.Name())
		}
	}

	return nil, fwk.NewStatus(fwk.Unschedulable, reasons...).WithPlugin(rejectorPlugin)
}

func (f *frameworkImpl) runPodGroupPostFilterPlugin(ctx context.Context, pl fwk.PodGroupPostFilterPlugin, state *framework.CycleState, podGroupInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PodGroupPostFilter(ctx, state, podGroupInfo, pgSchedulingFunc)
	}
	startTime := time.Now()
	res, status := pl.PodGroupPostFilter(ctx, state, podGroupInfo, pgSchedulingFunc)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PodGroupPostFilter, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return res, status
}

// RunFilterPluginsWithNominatedPods 在给定节点上针对提名 Pod 运行 Filter 插件。
// 调用方有两处：Schedule 与 Preempt。
// Schedule 调用时，要检验在当前节点所有 Pod 的基础上，再加上被提名到该节点运行的
// 同优先级及更高优先级 Pod 之后，该 Pod 是否仍可调度。
// Preempt 调用时，应先移除抢占受害者并加入提名 Pod；受害者的移除由
// SelectVictimsOnNode() 完成，Preempt 会在调用本函数前把它们从 PreFilter 状态与 NodeInfo 中移除。
func (f *frameworkImpl) RunFilterPluginsWithNominatedPods(ctx context.Context, state fwk.CycleState, pod *v1.Pod, info fwk.NodeInfo) *fwk.Status {
	var status *fwk.Status

	podsAdded := false
	// 某些情况下会跑两轮 Filter。若节点上有优先级大于等于当前 Pod 的提名 Pod，
	// 第一轮会把它们加入 PreFilter 状态与 nodeInfo；若第一轮全部通过，
	// 第二轮则在不加入这些提名 Pod 的情况下再跑一遍。第二轮是必要的，因为诸如 Pod 间亲和
	// 的过滤器在没有提名 Pod 时可能不通过。若节点没有提名 Pod，或第一轮失败，则不跑第二轮。
	// 第一轮只考虑同优先级及更高优先级的 Pod，因为当前 Pod 必须让位于它们，不能占用
	// 为它们预留的空间；占用低优先级 Pod 释放的资源则无妨。
	// 要求两种情况下都可调度，是为了保守决策：资源、Pod 间反亲和等过滤器在把提名 Pod
	// 视作运行时更可能失败，而 Pod 亲和在视作未运行时更可能失败。不能简单假定提名 Pod
	// 已在运行——它们此刻并未运行，甚至最终可能被调度到其他节点。

	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithName(logger, "FilterWithNominatedPods")
	ctx = klog.NewContext(ctx, logger)
	for i := 0; i < 2; i++ {
		stateToUse := state
		nodeInfoToUse := info
		if i == 0 {
			var err error
			podsAdded, stateToUse, nodeInfoToUse, err = addGENominatedPods(ctx, f, pod, state, info)
			if err != nil {
				return fwk.AsStatus(err)
			}
		} else if !podsAdded || !status.IsSuccess() {
			break
		}

		status = f.RunFilterPlugins(ctx, stateToUse, pod, nodeInfoToUse)
		if !status.IsSuccess() && !status.IsRejected() {
			return status
		}
	}

	return status
}

// addGENominatedPods 加入被提名到该节点、优先级大于等于当前 Pod 的 Pod，
// 返回 1）是否有 Pod 被加入；2）扩充后的 cycleState；3）扩充后的 nodeInfo。
func addGENominatedPods(ctx context.Context, fh fwk.Handle, pod *v1.Pod, state fwk.CycleState, nodeInfo fwk.NodeInfo) (bool, fwk.CycleState, fwk.NodeInfo, error) {
	if fh == nil {
		// 只可能发生在测试中。
		return false, state, nodeInfo, nil
	}
	nominatedPodInfos := fh.NominatedPodsForNode(nodeInfo.Node().Name)
	if len(nominatedPodInfos) == 0 {
		return false, state, nodeInfo, nil
	}
	nodeInfoOut := nodeInfo.Snapshot()
	stateOut := state.Clone()
	podsAdded := false
	for _, pi := range nominatedPodInfos {
		if corev1.PodPriority(pi.GetPod()) >= corev1.PodPriority(pod) && pi.GetPod().UID != pod.UID {
			nodeInfoOut.AddPodInfo(pi)
			status := fh.RunPreFilterExtensionAddPod(ctx, stateOut, pod, pi, nodeInfoOut)
			if !status.IsSuccess() {
				return false, state, nodeInfo, status.AsError()
			}
			podsAdded = true
		}
	}
	return podsAdded, stateOut, nodeInfoOut, nil
}

// RunPreScorePlugins 依次运行 PreScore 插件。任一插件返回 Success/Skip 之外的状态时，Pod 被拒绝。
// 返回 Skip 时，状态中的其他字段会被忽略，对应的 Score 插件在本调度周期中被跳过。
func (f *frameworkImpl) RunPreScorePlugins(
	ctx context.Context,
	state fwk.CycleState,
	pod *v1.Pod,
	nodes []fwk.NodeInfo,
) (status *fwk.Status) {
	startTime := time.Now()
	skipPlugins := sets.New[string]()
	defer func() {
		state.SetSkipScorePlugins(skipPlugins)
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PreScore, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PreScore")
	}
	for _, pl := range f.preScorePlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		status = f.runPreScorePlugin(ctx, pl, state, pod, nodes)
		if status.IsSkip() {
			skipPlugins.Insert(pl.Name())
			continue
		}
		if !status.IsSuccess() {
			return fwk.AsStatus(fmt.Errorf("running PreScore plugin %q: %w", pl.Name(), status.AsError()))
		}
	}
	return nil
}

func (f *frameworkImpl) runPreScorePlugin(ctx context.Context, pl fwk.PreScorePlugin, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PreScore(ctx, state, pod, nodes)
	}
	startTime := time.Now()
	status := pl.PreScore(ctx, state, pod, nodes)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PreScore, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunScorePlugins 运行配置的 Score 插件，返回各插件打分及每个节点的总分。
// 任一插件返回非成功状态时，返回的 *Status 也被置为非成功。
//
// 本函数与 RunPlacementScorePlugins 大体重复，修改其一时应考虑同步修改另一处。
func (f *frameworkImpl) RunScorePlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (ns []fwk.NodePluginScores, status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Score, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	allNodePluginScores := make([]fwk.NodePluginScores, len(nodes))
	plugins := f.nonSkippedScorePlugins(state)
	if len(plugins) == 0 {
		for i, nodeInfo := range nodes {
			allNodePluginScores[i] = fwk.NodePluginScores{
				Name:      nodeInfo.Node().Name,
				RawScores: make([]fwk.PluginScore, 0),
				Scores:    make([]fwk.PluginScore, 0),
			}
		}
		return allNodePluginScores, nil
	}

	pluginToNodeScores := make(map[string]fwk.NodeScoreList, len(plugins))
	for _, pl := range plugins {
		pluginToNodeScores[pl.Name()] = make(fwk.NodeScoreList, len(nodes))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := parallelize.NewResultChannel[error]()

	if l := klog.FromContext(ctx).V(4); l.Enabled() {
		ctx = klog.NewContext(ctx, klog.LoggerWithName(l, "Score"))
	}
	// 并行对每个节点运行 Score。
	f.Parallelizer().Until(ctx, len(nodes), func(index int) {
		nodeInfo := nodes[index]
		nodeName := nodeInfo.Node().Name
		nodeCtx := ctx
		if l := klog.FromContext(nodeCtx).V(4); l.Enabled() {
			nodeCtx = klog.NewContext(nodeCtx,
				klog.LoggerWithValues(l, "node", klog.ObjectRef{Name: nodeName}),
			)
		}
		rawScores, status := f.runRawScorePlugins(nodeCtx, state, pod, nodeInfo, plugins)
		if !status.IsSuccess() {
			errCh.SendWithCancel(status.AsError(), cancel)
			return
		}
		allNodePluginScores[index] = fwk.NodePluginScores{
			Name:      nodeName,
			RawScores: rawScores,
		}
		for _, rawScore := range rawScores {
			pluginToNodeScores[rawScore.Name][index] = fwk.NodeScore{
				Name:  nodeName,
				Score: rawScore.Score,
			}
		}
	}, metrics.Score)
	if err := errCh.Receive(); err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("running Score plugins: %w", err))
	}

	status = f.normalizeScores(ctx, state, pod, plugins, allNodePluginScores, pluginToNodeScores)
	if !status.IsSuccess() {
		return nil, status
	}
	return allNodePluginScores, nil
}

// RunRawScorePlugins 对单个节点运行所有未被跳过的 Score 插件，返回原始分数。
func (f *frameworkImpl) RunRawScorePlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) ([]fwk.PluginScore, *fwk.Status) {
	plugins := f.nonSkippedScorePlugins(state)
	if len(plugins) == 0 {
		return nil, nil
	}
	return f.runRawScorePlugins(ctx, state, pod, nodeInfo, plugins)
}

func (f *frameworkImpl) runRawScorePlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod,
	nodeInfo fwk.NodeInfo, plugins []fwk.ScorePlugin) ([]fwk.PluginScore, *fwk.Status) {
	scores := make([]fwk.PluginScore, 0, len(plugins))
	for _, pl := range plugins {
		plCtx := ctx
		if l := klog.FromContext(ctx).V(4); l.Enabled() {
			plCtx = klog.NewContext(ctx, klog.LoggerWithName(l, pl.Name()))
		}
		score, status := f.runScorePlugin(plCtx, pl, state, pod, nodeInfo)
		if !status.IsSuccess() {
			return nil, fwk.AsStatus(fmt.Errorf("plugin %q failed with: %w", pl.Name(), status.AsError()))
		}
		scores = append(scores, fwk.PluginScore{Name: pl.Name(), Score: score})
	}
	return scores, nil
}

// NormalizeScores 对给定的原始分数执行规整并应用权重，得到最终分数。
func (f *frameworkImpl) NormalizeScores(ctx context.Context, state fwk.CycleState, pod *v1.Pod,
	scores []fwk.NodePluginScores) *fwk.Status {
	plugins := f.nonSkippedScorePlugins(state)
	if len(plugins) == 0 {
		for i := range scores {
			scores[i].Scores = make([]fwk.PluginScore, 0)
			scores[i].TotalScore = 0
		}
		return nil
	}
	pluginToNodeScores := make(map[string]fwk.NodeScoreList, len(plugins))
	for _, pl := range plugins {
		pluginToNodeScores[pl.Name()] = make(fwk.NodeScoreList, len(scores))
	}

	for i, nodeScore := range scores {
		for _, rawScore := range nodeScore.RawScores {
			if nodeScoreList, ok := pluginToNodeScores[rawScore.Name]; ok {
				nodeScoreList[i] = fwk.NodeScore{
					Name:  nodeScore.Name,
					Score: rawScore.Score,
				}
			}
		}
	}

	return f.normalizeScores(ctx, state, pod, plugins, scores, pluginToNodeScores)
}

func (f *frameworkImpl) normalizeScores(ctx context.Context, state fwk.CycleState, pod *v1.Pod,
	plugins []fwk.ScorePlugin, scores []fwk.NodePluginScores, pluginToNodeScores map[string]fwk.NodeScoreList) *fwk.Status {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := parallelize.NewResultChannel[error]()

	// 预先把各插件的数据解析成切片，避免热路径上查 map。
	nodeScoreLists := make([]fwk.NodeScoreList, len(plugins))
	weights := make([]int, len(plugins))
	for i, pl := range plugins {
		nodeScoreLists[i] = pluginToNodeScores[pl.Name()]
		weights[i] = f.scorePluginWeight[pl.Name()]
	}

	// 并行对每个 ScorePlugin 运行 NormalizeScore。
	f.Parallelizer().Until(ctx, len(plugins), func(index int) {
		pl := plugins[index]
		if pl.ScoreExtensions() == nil {
			return
		}
		status := f.runScoreExtension(ctx, pl, state, pod, nodeScoreLists[index])
		if !status.IsSuccess() {
			err := fmt.Errorf("plugin %q failed with: %w", pl.Name(), status.AsError())
			errCh.SendWithCancel(err, cancel)
			return
		}
	}, metrics.Score)
	if err := errCh.Receive(); err != nil {
		return fwk.AsStatus(fmt.Errorf("running Normalize on Score plugins: %w", err))
	}

	// 并行对每个 ScorePlugin 应用分数权重。
	f.Parallelizer().Until(ctx, len(scores), func(index int) {
		nodePluginScores := make([]fwk.PluginScore, len(plugins))
		var totalScore int64
		for i, pl := range plugins {
			score := nodeScoreLists[i][index].Score

			if score > fwk.MaxScore || score < fwk.MinScore {
				err := fmt.Errorf("plugin %q returns an invalid score %v, it should in the range of [%v, %v] after normalizing",
					pl.Name(), score, fwk.MinScore, fwk.MaxScore)
				errCh.SendWithCancel(err, cancel)
				return
			}
			weightedScore := score * int64(weights[i])
			nodePluginScores[i] = fwk.PluginScore{Name: pl.Name(), Score: weightedScore}
			totalScore += weightedScore
		}
		scores[index].Scores = nodePluginScores
		scores[index].TotalScore = totalScore
	}, metrics.Score)
	if err := errCh.Receive(); err != nil {
		return fwk.AsStatus(fmt.Errorf("applying score defaultWeights on Score plugins: %w", err))
	}

	return nil
}

// nonSkippedScorePlugins 返回 cycleState 中未被标记为跳过的 Score 插件。
func (f *frameworkImpl) nonSkippedScorePlugins(state fwk.CycleState) []fwk.ScorePlugin {
	plugins := make([]fwk.ScorePlugin, 0, len(f.scorePlugins))
	for _, pl := range f.scorePlugins {
		if !state.GetSkipScorePlugins().Has(pl.Name()) {
			plugins = append(plugins, pl)
		}
	}
	return plugins
}

func (f *frameworkImpl) runScorePlugin(ctx context.Context, pl fwk.ScorePlugin, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.Score(ctx, state, pod, nodeInfo)
	}
	startTime := time.Now()
	s, status := pl.Score(ctx, state, pod, nodeInfo)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.Score, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return s, status
}

func (f *frameworkImpl) runScoreExtension(ctx context.Context, pl fwk.ScorePlugin, state fwk.CycleState, pod *v1.Pod, nodeScoreList fwk.NodeScoreList) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.ScoreExtensions().NormalizeScore(ctx, state, pod, nodeScoreList)
	}
	startTime := time.Now()
	status := pl.ScoreExtensions().NormalizeScore(ctx, state, pod, nodeScoreList)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.ScoreExtensionNormalize, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunPlacementScorePlugins 运行配置的 PlacementScore 插件，返回各插件打分及每个 Placement 的总分。
// 任一插件返回非成功状态时，返回的 *Status 也被置为非成功。
//
// placementStates 与 podGroupAssignments 按下标一一对应：placementStates[i] 是处理
// podGroupAssignments[i] 时使用的状态，并作为该 Placement 的 ScorePlacement 入参传入。
//
// 本函数与 RunScorePlugins 大体重复，修改其一时应考虑同步修改另一处。
func (f *frameworkImpl) RunPlacementScorePlugins(ctx context.Context, state fwk.PodGroupCycleState, podGroupInfo fwk.PodGroupInfo, podGroupAssignments []*fwk.PodGroupAssignments, placementStates []fwk.PlacementCycleState) (ps []fwk.PlacementPluginScores, status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PlacementScore, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()

	if len(podGroupAssignments) != len(placementStates) {
		return nil, fwk.AsStatus(fmt.Errorf("expected one PlacementCycleState per PodGroupAssignments, got %d placement states for %d assignments", len(placementStates), len(podGroupAssignments)))
	}

	allPlacementPluginScores := make([]fwk.PlacementPluginScores, len(podGroupAssignments))
	numPlugins := len(f.placementScorePlugins)
	plugins := make([]fwk.PlacementScorePlugin, 0, numPlugins)
	pluginToPlacementScores := make(map[string][]fwk.PlacementScore, numPlugins)
	for _, pl := range f.placementScorePlugins {
		plugins = append(plugins, pl)
		pluginToPlacementScores[pl.Name()] = make([]fwk.PlacementScore, len(podGroupAssignments))
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := parallelize.NewResultChannel[error]()

	if len(plugins) > 0 {
		logger := klog.FromContext(ctx)
		verboseLogs := logger.V(4).Enabled()
		if verboseLogs {
			logger = klog.LoggerWithName(logger, "PlacementScore")
		}
		// 并行对每个 Placement 运行 ScorePlacement。
		f.Parallelizer().Until(ctx, len(podGroupAssignments), func(index int) {
			pga := podGroupAssignments[index]
			logger := logger
			if verboseLogs {
				logger = klog.LoggerWithValues(logger, "placement", pga.Placement.Name)
			}
			for _, pl := range plugins {
				ctx := ctx
				if verboseLogs {
					logger := klog.LoggerWithName(logger, pl.Name())
					ctx = klog.NewContext(ctx, logger)
				}
				s, status := f.runPlacementScorePlugin(ctx, pl, placementStates[index], podGroupInfo, pga)
				if !status.IsSuccess() {
					err := fmt.Errorf("plugin %q failed with: %w", pl.Name(), status.AsError())
					errCh.SendWithCancel(err, cancel)
					return
				}
				pluginToPlacementScores[pl.Name()][index] = fwk.PlacementScore{
					Placement: pga.Placement,
					Score:     s,
				}
			}
		}, metrics.PlacementScore)
		if err := errCh.Receive(); err != nil {
			return nil, fwk.AsStatus(fmt.Errorf("running PlacementScore plugins: %w", err))
		}
	}

	// 并行对每个 PlacementScorePlugin 运行 NormalizePlacementScore。
	f.Parallelizer().Until(ctx, len(plugins), func(index int) {
		pl := plugins[index]
		if pl.PlacementScoreExtensions() == nil {
			return
		}
		placementScoreList := pluginToPlacementScores[pl.Name()]
		status := f.runPlacementScoreExtension(ctx, pl, state, podGroupInfo, placementScoreList)
		if !status.IsSuccess() {
			err := fmt.Errorf("plugin %q failed with: %w", pl.Name(), status.AsError())
			errCh.SendWithCancel(err, cancel)
			return
		}
	}, metrics.PlacementScore)
	if err := errCh.Receive(); err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("running NormalizePlacementScore on PlacementScore plugins: %w", err))
	}

	// 并行对每个 PlacementScorePlugin 应用分数权重，
	// 然后构建 allPlacementPluginScores。
	f.Parallelizer().Until(ctx, len(podGroupAssignments), func(index int) {
		placementPluginScores := fwk.PlacementPluginScores{
			Placement: podGroupAssignments[index].Placement,
			Scores:    make([]fwk.PluginScore, len(plugins)),
		}

		for i, pl := range plugins {
			weight := f.placementScorePluginWeight[pl.Name()]
			placementScoreList := pluginToPlacementScores[pl.Name()]
			score := placementScoreList[index].Score

			if score > fwk.MaxScore || score < fwk.MinScore {
				err := fmt.Errorf("plugin %q returns an invalid score %v, it should in the range of [%v, %v] after normalizing", pl.Name(), score, fwk.MinScore, fwk.MaxScore)
				errCh.SendWithCancel(err, cancel)
				return
			}
			weightedScore := score * int64(weight)
			placementPluginScores.Scores[i] = fwk.PluginScore{
				Name:  pl.Name(),
				Score: weightedScore,
			}
			placementPluginScores.TotalScore += weightedScore
		}
		allPlacementPluginScores[index] = placementPluginScores
	}, metrics.PlacementScore)
	if err := errCh.Receive(); err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("applying score defaultWeights on PlacementScore plugins: %w", err))
	}

	return allPlacementPluginScores, nil
}

func (f *frameworkImpl) runPlacementScorePlugin(ctx context.Context, pl fwk.PlacementScorePlugin, state fwk.PlacementCycleState, podGroup fwk.PodGroupInfo, placement *fwk.PodGroupAssignments) (int64, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.ScorePlacement(ctx, state, podGroup, placement)
	}
	startTime := time.Now()
	s, status := pl.ScorePlacement(ctx, state, podGroup, placement)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PlacementScore, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return s, status
}

func (f *frameworkImpl) runPlacementScoreExtension(ctx context.Context, pl fwk.PlacementScorePlugin, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, placementScoreList []fwk.PlacementScore) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PlacementScoreExtensions().NormalizePlacementScore(ctx, state, podGroup, placementScoreList)
	}
	startTime := time.Now()
	status := pl.PlacementScoreExtensions().NormalizePlacementScore(ctx, state, podGroup, placementScoreList)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PlacementScoreExtensionNormalize, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// GetNodeHint 返回批处理机制为给定 Pod 签名推荐的节点。
func (f *frameworkImpl) GetNodeHint(ctx context.Context, pod *v1.Pod, signature fwk.PodSignature, state fwk.CycleState, cycleCount int64) string {
	return f.batch.GetNodeHint(ctx, pod, signature, state, cycleCount)
}

// StoreScheduleResults 将调度结果写入批处理缓存。
func (f *frameworkImpl) StoreScheduleResults(ctx context.Context, signature fwk.PodSignature, hintedNode, chosenNode string, otherNodes framework.SortedScoredNodes, cycleCount int64) {
	f.batch.StoreScheduleResults(ctx, signature, hintedNode, chosenNode, otherNodes, cycleCount)
}

// RunPreBindPlugins 运行配置的 PreBind 插件。任一插件出错时返回失败状态，
// 其中包含拒绝原因或插件内部错误。
func (f *frameworkImpl) RunPreBindPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PreBind, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PreBind")
		logger = klog.LoggerWithValues(logger, "node", klog.ObjectRef{Name: nodeName})
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("RunPreBindPlugins has completed"))
	statusCh := parallelize.NewResultChannel[*fwk.Status]()
	skippedPlugins := state.GetSkipPreBindPlugins()
	parallelPlugins := state.GetParallelPreBindPlugins()

	// run 直接对单个插件执行。
	run := func(pl fwk.PreBindPlugin) *fwk.Status {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		plStatus := f.runPreBindPlugin(ctx, pl, state, pod, nodeName)
		if !plStatus.IsSuccess() {
			if plStatus.IsRejected() {
				logger.V(4).Info("Pod rejected by PreBind plugin", "pod", klog.KObj(pod), "node", nodeName, "plugin", pl.Name(), "status", plStatus.Message())
				plStatus.SetPlugin(pl.Name())
				return plStatus
			}
			err := plStatus.AsError()
			if errors.Is(err, context.Canceled) {
				err = context.Cause(ctx)
			}

			logger.Error(err, "Plugin failed", "plugin", pl.Name(), "pod", klog.KObj(pod), "node", nodeName)
			return fwk.AsStatus(fmt.Errorf("running PreBind plugin %q: %w", pl.Name(), err))
		}
		return nil
	}

	pluginGroups := f.getPreBindPluginGroups(state, skippedPlugins, parallelPlugins)
	for _, plugins := range pluginGroups {
		// 组内只有一个插件时，单独起 goroutine 毫无收益，直接同步执行。

		if len(plugins) == 1 {
			if status := run(plugins[0]); status != nil {
				return status
			}
			continue
		}
		f.Parallelizer().Until(ctx, len(plugins), func(index int) {
			if status := run(plugins[index]); !status.IsSuccess() {
				statusCh.SendWithCancel(status, func() {
					cancel(errors.New("some other PreBind operation failed"))
				})
			}
		}, metrics.PreBind)
		if status := statusCh.Receive(); !status.IsSuccess() {
			return status
		}
	}
	return nil
}

// getPreBindPluginGroups 把 PreBind 插件切分为可并行执行的组，
// 返回结果只包含未被跳过的插件。
func (f *frameworkImpl) getPreBindPluginGroups(state fwk.CycleState, skippedPlugins, parallelPlugins sets.Set[string]) [][]fwk.PreBindPlugin {
	var groups [][]fwk.PreBindPlugin
	var group []fwk.PreBindPlugin
	for _, pl := range f.preBindPlugins {
		if parallelPlugins.Has(pl.Name()) {
			if !skippedPlugins.Has(pl.Name()) {
				group = append(group, pl)
			}
			continue
		}
		if len(group) > 0 {
			groups = append(groups, group)
			group = nil
		}
		if !skippedPlugins.Has(pl.Name()) {
			groups = append(groups, []fwk.PreBindPlugin{pl})
		}
	}
	if len(group) > 0 {
		groups = append(groups, group)
	}
	return groups
}

func (f *frameworkImpl) runPreBindPlugin(ctx context.Context, pl fwk.PreBindPlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PreBind(ctx, state, pod, nodeName)
	}
	startTime := time.Now()
	status := pl.PreBind(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PreBind, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunPreBindPreFlights 依次运行 PreBind 插件的 PreBindPreFlight 函数。返回值含义：
// - Success：至少一个插件返回成功，即有 PreBind 插件会处理该 Pod；
// - Skip：所有插件都返回 Skip；
// - Error：任一插件返回错误。
func (f *frameworkImpl) RunPreBindPreFlights(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PreBindPreFlight, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PreBindPreFlight")
		logger = klog.LoggerWithValues(logger, "node", klog.ObjectRef{Name: nodeName})
	}
	skipPlugins := sets.New[string]()
	parallelPlugins := sets.New[string]()
	returningStatus := fwk.NewStatus(fwk.Skip)
	for _, pl := range f.preBindPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		result, status := f.runPreBindPreFlight(ctx, pl, state, pod, nodeName)
		if result != nil && result.AllowParallel {
			parallelPlugins.Insert(pl.Name())
		}
		switch {
		case status.Code() == fwk.Error:
			err := status.AsError()
			logger.Error(err, "Plugin failed", "plugin", pl.Name(), "pod", klog.KObj(pod), "node", nodeName)
			return fwk.AsStatus(fmt.Errorf("running PreBindPreFlight %q: %w", pl.Name(), err))
		case status.IsSuccess():
			// 只要有一个插件返回成功，整体即为成功。
			returningStatus = nil
		case status.IsSkip():
			skipPlugins.Insert(pl.Name())
		default:
			// 其他状态均属意外。
			return fwk.AsStatus(fmt.Errorf("PreBindPreFlight %s returned %q, which is unsupported. It is supposed to return Success, Skip, or Error status", pl.Name(), status.Code()))
		}
	}
	state.SetSkipPreBindPlugins(skipPlugins)
	state.SetParallelPreBindPlugins(parallelPlugins)
	return returningStatus
}

func (f *frameworkImpl) runPreBindPreFlight(ctx context.Context, pl fwk.PreBindPlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PreBindPreFlight(ctx, state, pod, nodeName)
	}
	startTime := time.Now()
	result, status := pl.PreBindPreFlight(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PreBindPreFlight, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return result, status
}

// RunBindPlugins 依次运行 Bind 插件，直到某个返回非 Skip 状态。
func (f *frameworkImpl) RunBindPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Bind, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	if len(f.bindPlugins) == 0 {
		return fwk.NewStatus(fwk.Skip, "")
	}
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "Bind")
	}
	for _, pl := range f.bindPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		status = f.runBindPlugin(ctx, pl, state, pod, nodeName)
		if status.IsSkip() {
			continue
		}
		if !status.IsSuccess() {
			if status.IsRejected() {
				logger.V(4).Info("Pod rejected by Bind plugin", "pod", klog.KObj(pod), "node", nodeName, "plugin", pl.Name(), "status", status.Message())
				status.SetPlugin(pl.Name())
				return status
			}
			err := status.AsError()
			logger.Error(err, "Plugin Failed", "plugin", pl.Name(), "pod", klog.KObj(pod), "node", nodeName)
			return fwk.AsStatus(fmt.Errorf("running Bind plugin %q: %w", pl.Name(), err))
		}
		return status
	}
	return status
}

func (f *frameworkImpl) runBindPlugin(ctx context.Context, bp fwk.BindPlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return bp.Bind(ctx, state, pod, nodeName)
	}
	startTime := time.Now()
	status := bp.Bind(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.Bind, bp.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunPostBindPlugins 运行配置的 PostBind 插件。
func (f *frameworkImpl) RunPostBindPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PostBind, fwk.Success.String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "PostBind")
	}
	for _, pl := range f.postBindPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		f.runPostBindPlugin(ctx, pl, state, pod, nodeName)
	}
}

func (f *frameworkImpl) runPostBindPlugin(ctx context.Context, pl fwk.PostBindPlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	if !state.ShouldRecordPluginMetrics() {
		pl.PostBind(ctx, state, pod, nodeName)
		return
	}
	startTime := time.Now()
	pl.PostBind(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PostBind, pl.Name(), fwk.Success.String(), metrics.SinceInSeconds(startTime))
}

// RunReservePluginsReserve 运行各 Reserve 插件的 Reserve 方法。任一插件出错即中止
// 后续插件并返回错误。此时 Pod 不会被调度，调用方需负责调用 RunReservePluginsUnreserve 回滚。
func (f *frameworkImpl) RunReservePluginsReserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Reserve, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "Reserve")
		logger = klog.LoggerWithValues(logger, "node", klog.ObjectRef{Name: nodeName})
	}
	for _, pl := range f.reservePlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		status = f.runReservePluginReserve(ctx, pl, state, pod, nodeName)
		if !status.IsSuccess() {
			if status.IsRejected() {
				logger.V(4).Info("Pod rejected by plugin", "pod", klog.KObj(pod), "plugin", pl.Name(), "status", status.Message())
				status.SetPlugin(pl.Name())
				return status
			}
			err := status.AsError()
			logger.Error(err, "Plugin failed", "plugin", pl.Name(), "pod", klog.KObj(pod))
			return fwk.AsStatus(fmt.Errorf("running Reserve plugin %q: %w", pl.Name(), err))
		}
	}
	return nil
}

func (f *frameworkImpl) runReservePluginReserve(ctx context.Context, pl fwk.ReservePlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.Reserve(ctx, state, pod, nodeName)
	}
	startTime := time.Now()
	status := pl.Reserve(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.Reserve, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// RunReservePluginsUnreserve 运行各 Reserve 插件的 Unreserve 方法。
func (f *frameworkImpl) RunReservePluginsUnreserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Unreserve, fwk.Success.String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	// 按 Reserve 执行的*逆序*调用各插件的 Unreserve。

	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "Unreserve")
		logger = klog.LoggerWithValues(logger, "node", klog.ObjectRef{Name: nodeName})
	}
	for i := len(f.reservePlugins) - 1; i >= 0; i-- {
		pl := f.reservePlugins[i]
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		f.runReservePluginUnreserve(ctx, pl, state, pod, nodeName)
	}
}

func (f *frameworkImpl) runReservePluginUnreserve(ctx context.Context, pl fwk.ReservePlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	if !state.ShouldRecordPluginMetrics() {
		pl.Unreserve(ctx, state, pod, nodeName)
		return
	}
	startTime := time.Now()
	pl.Unreserve(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.Unreserve, pl.Name(), fwk.Success.String(), metrics.SinceInSeconds(startTime))
}

// RunPermitPlugins 运行配置的 Permit 插件。任一插件返回 Success/Wait 之外的状态时，
// 中止后续插件并返回错误；若有插件返回 Wait，则构造 pluginsWaitTime 并返回 Wait 状态。
// 本函数不会创建等待中的 Pod 对象，调用方需自行调用 AddWaitingPod。
func (f *frameworkImpl) RunPermitPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (pluginsWaitTime map[string]time.Duration, status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Permit, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()
	var waitStatus *fwk.Status
	pluginsWaitTime = make(map[string]time.Duration)
	logger := klog.FromContext(ctx)
	verboseLogs := logger.V(4).Enabled()
	if verboseLogs {
		logger = klog.LoggerWithName(logger, "Permit")
		logger = klog.LoggerWithValues(logger, "node", klog.ObjectRef{Name: nodeName})
	}
	for _, pl := range f.permitPlugins {
		ctx := ctx
		if verboseLogs {
			logger := klog.LoggerWithName(logger, pl.Name())
			ctx = klog.NewContext(ctx, logger)
		}
		status, timeout := f.runPermitPlugin(ctx, pl, state, pod, nodeName)
		if !status.IsSuccess() {
			if status.IsRejected() {
				logger.V(4).Info("Pod rejected by plugin", "pod", klog.KObj(pod), "plugin", pl.Name(), "status", status.Message())
				return nil, status.WithPlugin(pl.Name())
			}
			if status.IsWait() {
				// 超时不得超过 maxTimeout。
				if timeout > maxTimeout {
					timeout = maxTimeout
				}
				pluginsWaitTime[pl.Name()] = timeout
				waitStatus = status
			} else {
				err := status.AsError()
				logger.Error(err, "Plugin failed", "plugin", pl.Name(), "pod", klog.KObj(pod))
				return nil, fwk.AsStatus(fmt.Errorf("running Permit plugin %q: %w", pl.Name(), err)).WithPlugin(pl.Name())
			}
		}
	}
	if waitStatus.IsWait() {
		logger.V(4).Info("One or more plugins asked to wait and no plugin rejected pod", "pod", klog.KObj(pod))
		waitStatus.AppendReason("one or more plugins asked to wait and no plugin rejected pod")
		return pluginsWaitTime, waitStatus
	}
	return nil, nil
}

func (f *frameworkImpl) runPermitPlugin(ctx context.Context, pl fwk.PermitPlugin, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.Permit(ctx, state, pod, nodeName)
	}
	startTime := time.Now()
	status, timeout := pl.Permit(ctx, state, pod, nodeName)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.Permit, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status, timeout
}

// RunPlacementFeasiblePlugins 运行实现了 PlacementFeasible 接口的插件。
// 全部返回 Success 时结果为 Success；其他合法状态只有 Wait 与 Unschedulable。
// 任一插件返回非法状态时结果为 Error，后续插件不再调用；
// 否则只要有一个返回 Unschedulable，后续插件不再调用且结果为 Unschedulable；
// 否则只要有一个返回 Wait，仍调用后续插件，结果为 Wait。
func (f *frameworkImpl) RunPlacementFeasiblePlugins(ctx context.Context, placementCycleState fwk.PlacementCycleState, podGroupInfo fwk.PodGroupInfo, args framework.PlacementProgress) (status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PlacementFeasible, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()

	for _, pl := range f.placementFeasiblePlugins {
		plStatus := f.runPlacementFeasiblePlugin(ctx, pl, placementCycleState, podGroupInfo, args)
		if plStatus.IsSuccess() {
			continue
		}
		if plStatus.Code() == fwk.Wait {
			status = plStatus.WithPlugin(pl.Name())
			continue
		}
		if plStatus.Code() == fwk.Unschedulable {
			return plStatus.WithPlugin(pl.Name())
		}
		if plStatus.IsError() {
			return fwk.AsStatus(fmt.Errorf("running PlacementFeasible plugin: %w", plStatus.AsError())).WithPlugin(pl.Name())
		}
		return fwk.AsStatus(fmt.Errorf("unexpected status from PlacementFeasible plugin: %v", plStatus.Code())).WithPlugin(pl.Name())
	}

	return status
}

func (f *frameworkImpl) runPlacementFeasiblePlugin(ctx context.Context, pl framework.PlacementFeasiblePlugin, state fwk.PlacementCycleState, podGroup fwk.PodGroupInfo, args framework.PlacementProgress) *fwk.Status {
	if !state.ShouldRecordPluginMetrics() {
		return pl.PlacementFeasible(ctx, state, podGroup, args)
	}
	startTime := time.Now()
	status := pl.PlacementFeasible(ctx, state, podGroup, args)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PlacementFeasible, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return status
}

// AddWaitingPod 创建一个等待中的 Pod 并加入 framework。
// 参数取自 RunPermitPlugins 返回的 pluginsWaitTime，Pod 将等待 Permit 插件要求的最短时间。
// 仅当 RunPermitPlugins 返回 Wait 且即将调用 WaitOnPermit 时才应调用本方法。
func (f *frameworkImpl) AddWaitingPod(pod *v1.Pod, pluginsWaitTime map[string]time.Duration) {
	waitingPod := newWaitingPod(pod, pluginsWaitTime)
	f.waitingPods.add(waitingPod)
}

// RunPlacementGeneratePlugins 运行配置的 PlacementGeneratePlugin 并返回生成的 Placement。
// 未配置插件时直接返回输入的 Placement。
func (f *frameworkImpl) RunPlacementGeneratePlugins(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, nodes []fwk.NodeInfo) (placements []*fwk.Placement, status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.PlacementGenerate, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	}()

	placement := &fwk.Placement{
		Nodes: nodes,
	}

	if len(f.placementGeneratePlugins) == 0 {
		return []*fwk.Placement{placement}, nil
	}

	plugin := f.placementGeneratePlugins[0]

	result, status := f.runPlacementGeneratePlugin(ctx, plugin, state, podGroup, placement)
	if !status.IsSuccess() {
		return nil, status.WithPlugin(plugin.Name())
	}

	if len(result.Placements) == 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "no feasible placements found").WithPlugin(plugin.Name())
	}

	return result.Placements, nil
}

func (f *frameworkImpl) runPlacementGeneratePlugin(ctx context.Context, pl fwk.PlacementGeneratePlugin, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, parentPlacement *fwk.Placement) (*fwk.GeneratePlacementsResult, *fwk.Status) {
	if !state.ShouldRecordPluginMetrics() {
		return pl.GeneratePlacements(ctx, state, podGroup, parentPlacement)
	}
	startTime := time.Now()
	placements, status := pl.GeneratePlacements(ctx, state, podGroup, parentPlacement)
	f.metricsRecorder.ObservePluginDurationAsync(metrics.PlacementGenerate, pl.Name(), status.Code().String(), metrics.SinceInSeconds(startTime))
	return placements, status
}

// WillWaitOnPermit 判断 Pod 是否正处于等待 Permit 的状态。
func (f *frameworkImpl) WillWaitOnPermit(ctx context.Context, pod *v1.Pod) bool {
	return f.waitingPods.get(pod.UID) != nil
}

// WaitOnPermit 在 Pod 处于等待状态时阻塞，直到其被拒绝或放行。
func (f *frameworkImpl) WaitOnPermit(ctx context.Context, pod *v1.Pod) *fwk.Status {
	waitingPod := f.waitingPods.get(pod.UID)
	if waitingPod == nil {
		return nil
	}
	defer f.waitingPods.remove(pod.UID)

	logger := klog.FromContext(ctx)
	logger.V(4).Info("Pod waiting on permit", "pod", klog.KObj(pod))

	startTime := time.Now()
	s := <-waitingPod.s
	metrics.PermitWaitDuration.WithLabelValues(s.Code().String()).Observe(metrics.SinceInSeconds(startTime))

	if !s.IsSuccess() {
		if s.IsRejected() {
			logger.V(4).Info("Pod rejected while waiting on permit", "pod", klog.KObj(pod), "status", s.Message())
			return s
		}
		err := s.AsError()
		logger.Error(err, "Failed waiting on permit for pod", "pod", klog.KObj(pod))
		return fwk.AsStatus(fmt.Errorf("waiting on permit for pod: %w", err)).WithPlugin(s.Plugin())
	}
	return nil
}

// SnapshotSharedLister 返回最新 NodeInfo 快照的 SharedLister。
// 快照在调度周期开始时生成，并在 Pod 完成 Reserve 前保持不变；Reserve 之后不保证信息不变。
func (f *frameworkImpl) SnapshotSharedLister() fwk.SharedLister {
	return f.snapshotSharedLister
}

// MutableSnapshotSharedLister 返回最新 NodeInfo 快照的可变 SharedLister。
// 注意：仅 PodGroupPostFilter 扩展点可以使用。
func (f *frameworkImpl) MutableSnapshotSharedLister() fwk.MutableSnapshotSharedLister {
	return f.mutableSnapshotLister
}

// IterateOverWaitingPods 获取读锁并遍历 WaitingPods。
func (f *frameworkImpl) IterateOverWaitingPods(callback func(fwk.WaitingPod)) {
	f.waitingPods.iterate(callback)
}

// GetWaitingPod 按 UID 返回对应的 WaitingPod。
func (f *frameworkImpl) GetWaitingPod(uid types.UID) fwk.WaitingPod {
	if wp := f.waitingPods.get(uid); wp != nil {
		return wp
	}
	return nil // Returning nil instead of *waitingPod(nil).
}

// RejectWaitingPod 按 UID 拒绝一个 WaitingPod，返回值表示拒绝是否成功。
func (f *frameworkImpl) RejectWaitingPod(uid types.UID) bool {
	if waitingPod := f.waitingPods.get(uid); waitingPod != nil {
		return waitingPod.Reject("", "removed")
	}
	return false
}

// AddPodInPreBind 把 Pod 加入 preBind 列表。
func (f *frameworkImpl) AddPodInPreBind(uid types.UID, cancel context.CancelCauseFunc) {
	f.podsInPreBind.add(uid, cancel)
}

// GetPodInPreBind 按 UID 返回处于绑定周期但尚未绑定的 Pod。
func (f *frameworkImpl) GetPodInPreBind(uid types.UID) fwk.PodInPreBind {
	if bp := f.podsInPreBind.get(uid); bp != nil {
		return bp
	}
	return nil
}

// RemovePodInPreBind 从 preBind 列表中移除 Pod。
func (f *frameworkImpl) RemovePodInPreBind(uid types.UID) {
	f.podsInPreBind.remove(uid)
}

// HasFilterPlugins 判断是否至少配置了一个 Filter 插件。
func (f *frameworkImpl) HasFilterPlugins() bool {
	return len(f.filterPlugins) > 0
}

// HasPostFilterPlugins 判断是否至少配置了一个 PostFilter 插件。
func (f *frameworkImpl) HasPostFilterPlugins() bool {
	return len(f.postFilterPlugins) > 0
}

// HasScorePlugins 判断是否至少配置了一个 Score 插件。
func (f *frameworkImpl) HasScorePlugins() bool {
	return len(f.scorePlugins) > 0
}

// PodGroupPostFilterPlugins 返回已注册的 PodGroupPostFilter 插件。
func (f *frameworkImpl) PodGroupPostFilterPlugins() []fwk.PodGroupPostFilterPlugin {
	return f.podGroupPostFilterPlugins
}

// ListPlugins 返回扩展点到该点所配置插件名的映射；未配置插件时返回 nil。
func (f *frameworkImpl) ListPlugins() *config.Plugins {
	m := config.Plugins{}

	for _, e := range f.getExtensionPoints(&m) {
		plugins := reflect.ValueOf(e.slicePtr).Elem()
		extName := plugins.Type().Elem().Name()
		var cfgs []config.Plugin
		for i := 0; i < plugins.Len(); i++ {
			name := plugins.Index(i).Interface().(fwk.Plugin).Name()
			p := config.Plugin{Name: name}
			// 权重仅适用于 Score 与 PlacementScore 插件。
			if extName == "ScorePlugin" {
				p.Weight = int32(f.scorePluginWeight[name])
			}
			if extName == "PlacementScorePlugin" {
				p.Weight = int32(f.placementScorePluginWeight[name])
			}
			cfgs = append(cfgs, p)
		}
		if len(cfgs) > 0 {
			e.plugins.Enabled = cfgs
		}
	}
	return &m
}

// ClientSet 返回 Kubernetes clientset。
func (f *frameworkImpl) ClientSet() clientset.Interface {
	return f.clientSet
}

// KubeConfig 返回 Kubernetes 客户端配置。
func (f *frameworkImpl) KubeConfig() *restclient.Config {
	return f.kubeConfig
}

// EventRecorder 返回事件记录器。
func (f *frameworkImpl) EventRecorder() events.EventRecorderLogger {
	return f.eventRecorder
}

// SharedInformerFactory 返回共享 informer 工厂。
func (f *frameworkImpl) SharedInformerFactory() informers.SharedInformerFactory {
	return f.informerFactory
}

// SharedDRAManager 返回 framework 的 SharedDRAManager。
func (f *frameworkImpl) SharedDRAManager() fwk.SharedDRAManager {
	return f.sharedDRAManager
}

// SharedCSIManager 返回 framework 的 SharedCSIManager。
func (f *frameworkImpl) SharedCSIManager() fwk.CSIManager {
	return f.sharedCSIManager
}

// PodGroupManager 返回 framework 的 PodGroupManager。
func (f *frameworkImpl) PodGroupManager() fwk.PodGroupManager {
	return f.podGroupManager
}

func (f *frameworkImpl) pluginsNeeded(plugins *config.Plugins) sets.Set[string] {
	pgSet := sets.Set[string]{}

	if plugins == nil {
		return pgSet
	}

	find := func(pgs *config.PluginSet) {
		for _, pg := range pgs.Enabled {
			pgSet.Insert(pg.Name)
		}
	}

	for _, e := range f.getExtensionPoints(plugins) {
		find(e.plugins)
	}
	// MultiPoint 不在 getExtensionPoints 返回的范围内，需单独处理。
	find(&plugins.MultiPoint)

	return pgSet
}

// ProfileName 返回该 framework 关联的 profile 名称。
func (f *frameworkImpl) ProfileName() string {
	return f.profileName
}

// PercentageOfNodesToScore 返回该 profile 的 percentageOfNodesToScore。
func (f *frameworkImpl) PercentageOfNodesToScore() *int32 {
	return f.percentageOfNodesToScore
}

// Parallelizer 返回保存调度器并行度设置的 parallelizer。
func (f *frameworkImpl) Parallelizer() fwk.Parallelizer {
	return f.parallelizer
}

// APIDispatcher 返回可用于分发 API 调用的 dispatcher。
// 需启用 SchedulerAsyncAPICalls 特性门控。
func (f *frameworkImpl) APIDispatcher() fwk.APIDispatcher {
	if f.apiDispatcher == nil {
		return nil
	}
	return f.apiDispatcher
}

// APICacher 返回可经由调度器缓存（而非直接使用 APIDispatcher()）分发 API 调用的 cacher。
// 需启用 SchedulerAsyncAPICalls 特性门控。
func (f *frameworkImpl) APICacher() fwk.APICacher {
	if f.apiCacher == nil {
		return nil
	}
	return f.apiCacher
}

// TotalBatchedPods 返回已批处理 Pod 的总数，仅用于测试。
func (f *frameworkImpl) TotalBatchedPods() int64 {
	return f.batch.batchedPods
}
