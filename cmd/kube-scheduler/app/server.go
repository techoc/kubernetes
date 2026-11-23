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

// Package app implements a Server object for running the scheduler.
// 本包连接命令行配置与 pkg/scheduler：负责配置校验、插件注册、Scheduler 实例化和服务运行。
package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	goruntime "runtime"
	"time"

	"github.com/blang/semver/v4"
	"github.com/spf13/cobra"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/server/statusz"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/component-base/term"
	utilversion "k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	schedulerserverconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	conversionv1 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics/resources"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

const (
	kubeScheduler = "kube-scheduler"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	utilruntime.Must(features.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// Option configures a framework.Registry.
// Option 是插件注册表扩展函数，调用方可以在启动前注入树外插件工厂。
type Option func(runtime.Registry) error

// NewSchedulerCommand 创建 kube-scheduler 命令；这里只准备 Options 和 Flag，实际启动发生在 RunE。
func NewSchedulerCommand(registryOptions ...Option) *cobra.Command {
	opts := options.NewOptions()

	cmd := &cobra.Command{
		Use: "kube-scheduler",
		Long: `The Kubernetes scheduler is a control plane process which assigns
Pods to Nodes. The scheduler determines which Nodes are valid placements for
each Pod in the scheduling queue according to constraints and available
resources. The scheduler then ranks each valid Node and binds the Pod to a
suitable Node. Multiple different schedulers may be used within a cluster;
kube-scheduler is the reference implementation.
See [scheduling](https://kubernetes.io/docs/concepts/scheduling-eviction/)
for more information about scheduling and the kube-scheduler component.`,
		// 在 RunE 之前应用组件版本和 Feature Gate，后续默认值和配置校验依赖这份全局状态。
		PersistentPreRunE: func(*cobra.Command, []string) error {
			// makes sure feature gates are set before RunE.
			return opts.ComponentGlobalsRegistry.Set()
		},
		// 参数解析成功后进入真正的服务启动流程。
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCommand(cmd, opts, registryOptions...)
		},
		// Scheduler 是常驻服务，不接收位置参数；拒绝非空参数可避免拼写错误被静默忽略。
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	nfs := opts.Flags
	verflag.AddFlags(nfs.FlagSet("global"))
	globalflag.AddGlobalFlags(nfs.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	fs := cmd.Flags()
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, *nfs, cols)

	if err := cmd.MarkFlagFilename("config", "yaml", "yml", "json"); err != nil {
		klog.Background().Error(err, "Failed to mark flag filename")
	}

	return cmd
}

// runCommand runs the scheduler.
func runCommand(cmd *cobra.Command, opts *options.Options, registryOptions ...Option) error {
	// 版本查询属于 CLI 快捷路径；请求版本信息时直接输出并退出，不进入服务启动。
	verflag.PrintAndExitIfRequested()
	fg := opts.ComponentGlobalsRegistry.FeatureGateFor(basecompatibility.DefaultKubeComponent)
	// Activate logging as soon as possible, after that
	// show flags with the final logging configuration.
	if err := logsapi.ValidateAndApply(opts.Logs, fg); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	// 记录最终解析的参数值，便于排查默认值、配置文件和命令行覆盖关系。
	cliflag.PrintFlags(cmd.Flags())

	if opts.InformerName == nil {
		informerName, err := cache.NewInformerName("kube-scheduler")
		if err != nil {
			return err
		}
		opts.InformerName = informerName
	}

	// 使用统一 Context 管理进程生命周期；收到 SIGTERM/SIGINT 后取消它，通知后台组件有序停止。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		stopCh := server.SetupSignalHandler()
		<-stopCh
		cancel()
	}()

	// Setup 完成配置校验、依赖创建、树外插件注册和 Scheduler 实例化。
	cc, sched, err := Setup(ctx, opts, registryOptions...)
	if err != nil {
		return err
	}
	// add feature enablement metrics
	fg.(featuregate.MutableFeatureGate).AddMetrics()
	// add component version metrics
	opts.ComponentGlobalsRegistry.AddMetrics()

	return Run(ctx, cc, sched)
}

// Run 根据已完成的配置启动 Scheduler 和 HTTP 服务，直到 Context 结束或发生致命错误。
func Run(ctx context.Context, cc *schedulerserverconfig.CompletedConfig, sched *scheduler.Scheduler) error {
	logger := klog.FromContext(ctx)

	// To help debugging, immediately log version
	logger.Info("Starting Kubernetes Scheduler", "version", utilversion.Get())

	logger.Info("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	// configz 对外展示版本化 API 对象，而内部运行时使用内部类型；先转换为 v1 再注册调试端点。
	externalConfig := &configv1.KubeSchedulerConfiguration{}
	if err := conversionv1.Convert_config_KubeSchedulerConfiguration_To_v1_KubeSchedulerConfiguration(&cc.ComponentConfig, externalConfig, nil); err != nil {
		return fmt.Errorf("unable to convert configz: %w", err)
	}
	externalConfig.SetGroupVersionKind(configv1.SchemeGroupVersion.WithKind("KubeSchedulerConfiguration"))

	// Configz registration.
	if cz, err := configz.New("componentconfig"); err != nil {
		return fmt.Errorf("unable to register configz: %w", err)
	} else if err := cz.Set(externalConfig); err != nil {
		return fmt.Errorf("unable to set configz: %w", err)
	}

	// 启动事件写回管道；Context 结束时停止记录，并由 defer 关闭广播器。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())
	defer cc.EventBroadcaster.Shutdown()

	// Setup healthz checks.
	var checks, readyzChecks []healthz.HealthChecker
	if cc.ComponentConfig.LeaderElection.LeaderElect {
		checks = append(checks, cc.LeaderElection.WatchDog)
		readyzChecks = append(readyzChecks, cc.LeaderElection.WatchDog)
	}
	readyzChecks = append(readyzChecks, healthz.NewShutdownHealthz(ctx.Done()))

	// 通道关闭表示本实例已经获得领导权；未关闭时表示仍在等待 Leader。
	waitingForLeader := make(chan struct{})
	isLeader := func() bool {
		select {
		case _, ok := <-waitingForLeader:
			// if channel is closed, we are leading
			return !ok
		default:
			// channel is open, we are waiting for a leader
			return false
		}
	}

	// 初始 LIST 对象不仅要进入缓存，还必须全部分发给事件处理器后才算真正就绪。
	// 通道关闭前 /readyz 持续失败，避免缓存未完整时接收调度工作。
	handlerSyncReadyCh := make(chan struct{})
	handlerSyncCheck := healthz.NamedCheck("sched-handler-sync", func(_ *http.Request) error {
		select {
		case <-handlerSyncReadyCh:
			return nil
		default:
		}
		return fmt.Errorf("handlers are not fully synchronized")
	})
	readyzChecks = append(readyzChecks, handlerSyncCheck)

	// 协同选主需要 LeaseCandidate 携带版本信息；只有启用选主且打开 Feature Gate 时才启动候选者控制器。
	if cc.LeaderElection != nil && utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CoordinatedLeaderElection) {
		binaryVersion, err := semver.ParseTolerant(cc.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent).BinaryVersion().String())
		if err != nil {
			return err
		}
		emulationVersion, err := semver.ParseTolerant(cc.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent).EmulationVersion().String())
		if err != nil {
			return err
		}

		// Start lease candidate controller for coordinated leader election
		leaseCandidate, waitForSync, err := leaderelection.NewCandidate(
			cc.Client,
			metav1.NamespaceSystem,
			cc.LeaderElection.Lock.Identity(),
			kubeScheduler,
			binaryVersion.FinalizeVersion(),
			emulationVersion.FinalizeVersion(),
			coordinationv1.OldestEmulationVersion,
		)
		if err != nil {
			return err
		}
		readyzChecks = append(readyzChecks, healthz.NewInformerSyncHealthz(waitForSync))
		go leaseCandidate.Run(ctx)
	}

	// Start up the server for endpoints.
	// 未启用 HTTPS 时关闭回调为空函数；启用后替换为停止监听并等待在途请求结束的清理过程。
	gracefulShutdownSecureServer := func() {}
	if cc.SecureServing != nil {
		handler := buildHandlerChain(newEndpointsHandler(&cc.ComponentConfig, cc.InformerFactory, isLeader, checks, readyzChecks, cc.Flagz), cc.Authentication.Authenticator, cc.Authorization.Authorizer)
		internalStopCh := make(chan struct{})
		shutdownTimeout := 5 * time.Second
		stoppedCh, listenerStoppedCh, err := cc.SecureServing.Serve(handler, shutdownTimeout, internalStopCh)
		if err != nil {
			// fail early for secure handlers, removing the old error loop from above
			close(internalStopCh)
			return fmt.Errorf("failed to start secure server: %v", err)
		}
		gracefulShutdownSecureServer = func() {
			close(internalStopCh)
			<-listenerStoppedCh
			logger.Info("[graceful-termination] secure server has stopped listening")
			<-stoppedCh
			logger.Info("[graceful-termination] secure server is exiting")
		}
	}

	// 将启动 Informer、同步缓存和等待事件处理器封装为一个过程，防止调度循环过早启动。
	startInformersAndWaitForSync := func(ctx context.Context) {
		// Start all informers.
		cc.InformerFactory.Start(ctx.Done())
		// DynInformerFactory can be nil in tests.
		if cc.DynInformerFactory != nil {
			cc.DynInformerFactory.Start(ctx.Done())
		}

		// WaitForCacheSync 确保 LIST/WATCH 已建立且本地 Store 完成首次填充，否则插件读取到的资源视图可能不完整。
		cc.InformerFactory.WaitForCacheSync(ctx.Done())
		// DynInformerFactory can be nil in tests.
		if cc.DynInformerFactory != nil {
			cc.DynInformerFactory.WaitForCacheSync(ctx.Done())
		}

		// 缓存同步不等于事件处理完成：还要等待初始对象全部送达 Scheduler Handler，否则 Pod 可能尚未进入调度队列。
		if err := sched.WaitForHandlersSync(ctx); err != nil {
			logger.Error(err, "handlers are not fully synchronized")
		}

		close(handlerSyncReadyCh)
		logger.V(3).Info("Handlers synced")
	}
	// 未启用延迟缓存时启动阶段立即同步；启用延迟缓存且配置选主时，把同步推迟到成为 Leader 之后。
	if !cc.ComponentConfig.DelayCacheUntilActive || cc.LeaderElection == nil {
		startInformersAndWaitForSync(ctx)
	}
	// If leader election is enabled, runCommand via LeaderElector until done and exit.
	if cc.LeaderElection != nil {
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CoordinatedLeaderElection) {
			cc.LeaderElection.Coordinated = true
		}
		cc.LeaderElection.Callbacks = leaderelection.LeaderCallbacks{
			// 获得 Lease 后标记为 Leader；必要时先启动并同步缓存，随后进入调度主循环。
			OnStartedLeading: func(ctx context.Context) {
				close(waitingForLeader)
				if cc.ComponentConfig.DelayCacheUntilActive {
					logger.Info("Starting informers and waiting for sync...")
					startInformersAndWaitForSync(ctx)
					logger.Info("Sync completed")
				}
				sched.Run(ctx)
			},
			// 停止领导后先关闭 HTTPS 服务；正常收到退出信号则成功退出，否则按 Lease 丢失处理。
			OnStoppedLeading: func() {
				gracefulShutdownSecureServer()
				select {
				case <-ctx.Done():
					// We were asked to terminate. Exit 0.
					logger.Info("Requested to terminate, exiting")
					os.Exit(0)
				default:
					// We lost the lock.
					logger.Error(nil, "Leaderelection lost")
					klog.FlushAndExit(klog.ExitFlushTimeout, 1)
				}
			},
		}
		leaderElector, err := leaderelection.NewLeaderElector(*cc.LeaderElection)
		if err != nil {
			return fmt.Errorf("couldn't create leader elector: %v", err)
		}

		// Run 阻塞到 Context 结束或选主回调导致退出；控制返回通常表示当前实例已失去租约。
		leaderElector.Run(ctx)

		return fmt.Errorf("lost lease")
	}

	// Leader election is disabled, so runCommand inline until done.
	close(waitingForLeader)
	sched.Run(ctx)
	gracefulShutdownSecureServer()
	return fmt.Errorf("finished without leader elect")
}

// buildHandlerChain 使用标准 HTTP 过滤器包装端点处理器，包含 Panic Recovery、日志、请求信息、认证和授权。
func buildHandlerChain(handler http.Handler, authn authenticator.Request, authz authorizer.UnconditionalAuthorizer) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}
	failedHandler := genericapifilters.Unauthorized(scheme.Codecs)

	handler = genericapifilters.WithAuthorization(handler, authz, scheme.Codecs)
	handler = genericapifilters.WithAuthentication(handler, authn, failedHandler, nil, nil)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

// installMetricHandler 安装组件配置和指标端点；资源指标依赖主实例缓存，因此仅在 Leader 上响应。
func installMetricHandler(pathRecorderMux *mux.PathRecorderMux, informers informers.SharedInformerFactory, isLeader func() bool) {
	configz.InstallHandler(pathRecorderMux)
	pathRecorderMux.Handle("/metrics", legacyregistry.HandlerWithReset())

	resourceMetricsHandler := resources.Handler(informers.Core().V1().Pods().Lister())
	pathRecorderMux.HandleFunc("/metrics/resources", func(w http.ResponseWriter, req *http.Request) {
		if !isLeader() {
			return
		}
		resourceMetricsHandler.ServeHTTP(w, req)
	})
}

// newEndpointsHandler creates an API health server from the config, and will also
// embed the metrics handler and z-pages handler.
// TODO: healthz check is deprecated, please use livez and readyz instead. Will be removed in the future.
func newEndpointsHandler(config *kubeschedulerconfig.KubeSchedulerConfiguration, informers informers.SharedInformerFactory, isLeader func() bool, healthzChecks, readyzChecks []healthz.HealthChecker, flagReader flagz.Reader) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux(kubeScheduler)
	healthz.InstallHandler(pathRecorderMux, healthzChecks...)
	healthz.InstallLivezHandler(pathRecorderMux)
	healthz.InstallReadyzHandler(pathRecorderMux, readyzChecks...)
	installMetricHandler(pathRecorderMux, informers, isLeader)
	slis.SLIMetricsWithReset{}.Install(pathRecorderMux)

	if config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routes.DebugFlags{}.Install(pathRecorderMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		if flagReader != nil {
			flagz.Install(pathRecorderMux, kubeScheduler, flagReader)
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		statusz.Install(pathRecorderMux, kubeScheduler, statusz.NewRegistry(compatibility.DefaultBuildEffectiveVersion(), statusz.WithListedPaths(pathRecorderMux.ListedPaths())))
	}

	return pathRecorderMux
}

// getRecorderFactory 为每个 Scheduler Profile 创建对应名称的事件记录器。
func getRecorderFactory(cc *schedulerserverconfig.CompletedConfig) profile.RecorderFactory {
	return func(name string) events.EventRecorderLogger {
		return cc.EventBroadcaster.NewRecorder(name)
	}
}

// WithPlugin creates an Option based on plugin name and factory. Please don't remove this function: it is used to register out-of-tree plugins,
// hence there are no references to it from the kubernetes scheduler code base.
func WithPlugin(name string, factory runtime.PluginFactory) Option {
	return func(registry runtime.Registry) error {
		return registry.Register(name, factory)
	}
}

// Setup 依次加载默认配置、校验 Options、创建运行时依赖、注册树外插件并实例化 Scheduler。
func Setup(ctx context.Context, opts *options.Options, outOfTreeRegistryOptions ...Option) (*schedulerserverconfig.CompletedConfig, *scheduler.Scheduler, error) {
	// 先装入当前 API 版本的默认配置；后续 Options.Config 可按用户的 --config 替换它。
	if cfg, err := latest.Default(); err != nil {
		return nil, nil, err
	} else {
		opts.ComponentConfig = cfg
	}

	// 配置无效时不创建客户端、Informer 等后台资源，并聚合返回全部错误。
	if errs := opts.Validate(); len(errs) > 0 {
		return nil, nil, utilerrors.NewAggregate(errs)
	}

	c, err := opts.Config(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Get the completed config
	cc := c.Complete()

	// 树外插件使用独立 Registry，在 Framework 构建前集中注册，避免修改内置插件注册表。
	outOfTreeRegistry := make(runtime.Registry)
	for _, option := range outOfTreeRegistryOptions {
		if err := option(outOfTreeRegistry); err != nil {
			return nil, nil, err
		}
	}

	recorderFactory := getRecorderFactory(&cc)
	completedProfiles := make([]kubeschedulerconfig.KubeSchedulerProfile, 0)
	// scheduler.New 按 Profile 构建 Framework、实例化插件，并连接调度队列和 Informer 事件。
	sched, err := scheduler.New(ctx,
		cc.Client,
		cc.InformerFactory,
		cc.DynInformerFactory,
		recorderFactory,
		scheduler.WithComponentConfigVersion(cc.ComponentConfig.TypeMeta.APIVersion),
		scheduler.WithKubeConfig(cc.KubeConfig),
		scheduler.WithProfiles(cc.ComponentConfig.Profiles...),
		scheduler.WithPercentageOfNodesToScore(cc.ComponentConfig.PercentageOfNodesToScore),
		scheduler.WithFrameworkOutOfTreeRegistry(outOfTreeRegistry),
		scheduler.WithPodMaxBackoffSeconds(cc.ComponentConfig.PodMaxBackoffSeconds),
		scheduler.WithPodInitialBackoffSeconds(cc.ComponentConfig.PodInitialBackoffSeconds),
		scheduler.WithPodMaxInUnschedulablePodsDuration(cc.PodMaxInUnschedulablePodsDuration),
		scheduler.WithExtenders(cc.ComponentConfig.Extenders...),
		scheduler.WithParallelism(cc.ComponentConfig.Parallelism),
		scheduler.WithBuildFrameworkCapturer(func(profile kubeschedulerconfig.KubeSchedulerProfile) {
			// Profiles are processed during Framework instantiation to set default plugins and configurations. Capturing them for logging
			completedProfiles = append(completedProfiles, profile)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	if err := options.LogOrWriteConfig(klog.FromContext(ctx), opts.WriteConfigTo, &cc.ComponentConfig, completedProfiles); err != nil {
		return nil, nil, err
	}

	return &cc, sched, nil
}
