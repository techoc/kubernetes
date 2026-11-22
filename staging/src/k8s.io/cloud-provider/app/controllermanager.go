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

package app

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/spf13/cobra"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/informers"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	cloudprovider "k8s.io/cloud-provider"
	cloudcontrollerconfig "k8s.io/cloud-provider/app/config"
	configv1alpha "k8s.io/cloud-provider/config/v1alpha1"
	"k8s.io/cloud-provider/names"
	"k8s.io/cloud-provider/options"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics/features"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/component-base/term"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	genericcontrollermanager "k8s.io/controller-manager/app"
	"k8s.io/controller-manager/controller"
	"k8s.io/controller-manager/pkg/clientbuilder"
	cmfeatures "k8s.io/controller-manager/pkg/features"
	controllerhealthz "k8s.io/controller-manager/pkg/healthz"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/controller-manager/pkg/leadermigration"
	"k8s.io/klog/v2"
)

func init() {
	// 在包初始化阶段注册特性门控，确保任何 flag 解析之前门控已可查询。
	utilruntime.Must(features.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

const (
	// ControllerStartJitter is the jitter value used when starting controller managers.
	// 依次启动控制器时加入 1.0 抖动，避免所有控制器在重启瞬间同时打 API。
	ControllerStartJitter = 1.0
	// ConfigzName is the name used for register cloud-controller manager /configz, same with GroupName.
	// 与配置 GroupName 相同，便于通过 /configz 直接识别组件类型。
	ConfigzName = "cloudcontrollermanager.config.k8s.io"
)

// NewCloudControllerManagerCommand creates a *cobra.Command object with default parameters
// controllerInitFuncConstructors is a map of controller name(as defined by controllers flag in https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/#options) to their InitFuncConstructor.
// additionalFlags provides controller specific flags to be included in the complete set of controller manager flags
// 这是不经过 CommandBuilder 的传统入口；与 builder 路径的区别在于 webhook 集合固定为空。
func NewCloudControllerManagerCommand(s *options.CloudControllerManagerOptions, cloudInitializer InitCloudFunc, controllerInitFuncConstructors map[string]ControllerInitFuncConstructor, controllerAliases map[string]string, additionalFlags cliflag.NamedFlagSets, stopCh <-chan struct{}) *cobra.Command {
	logOptions := logs.NewOptions()

	cmd := &cobra.Command{
		Use: "cloud-controller-manager",
		Long: `The Cloud controller manager is a daemon that embeds
the cloud specific control loops shipped with Kubernetes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()

			// Activate logging as soon as possible, after that
			// show flags with the final logging configuration.
			// 先应用日志配置再打印 flag，输出的日志选项与实际运行时一致。
			if err := logsapi.ValidateAndApply(logOptions, utilfeature.DefaultFeatureGate); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return err
			}
			cliflag.PrintFlags(cmd.Flags())

			c, err := s.Config(ControllerNames(controllerInitFuncConstructors), ControllersDisabledByDefault.List(), controllerAliases, AllWebhooks, DisabledByDefaultWebhooks)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return err
			}

			completedConfig := c.Complete()
			cloud := cloudInitializer(completedConfig)
			controllerInitializers := ConstructControllerInitializers(controllerInitFuncConstructors, completedConfig, cloud)

			if err := Run(completedConfig, cloud, controllerInitializers, make(map[string]WebhookHandler), stopCh); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return err
			}
			return nil
		},
		Args: func(cmd *cobra.Command, args []string) error {
			// 与 builder 路径一致：拒绝任何位置参数。
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	fs := cmd.Flags()
	namedFlagSets := s.Flags(ControllerNames(controllerInitFuncConstructors), ControllersDisabledByDefault.List(), controllerAliases, AllWebhooks, DisabledByDefaultWebhooks)

	globalFlagSet := namedFlagSets.FlagSet("global")
	verflag.AddFlags(globalFlagSet)
	logsapi.AddFlags(logOptions, globalFlagSet)
	globalflag.AddGlobalFlags(globalFlagSet, cmd.Name(), logs.SkipLoggingConfigurationFlags())

	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}
	for _, f := range additionalFlags.FlagSets {
		fs.AddFlagSet(f)
	}

	usageFmt := "Usage:\n  %s\n"
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStderr(), namedFlagSets, cols)
		cliflag.PrintSections(cmd.OutOrStderr(), additionalFlags, cols)
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStdout(), namedFlagSets, cols)
		cliflag.PrintSections(cmd.OutOrStdout(), additionalFlags, cols)
	})

	return cmd
}

// Run runs the ExternalCMServer.  This should never exit.
// 职责：建立事件与 metrics 管道、注册 /configz 与健康检查、按需启动 webhook 与安全服务端，
// 最后在选主成功（或未启用选主）时启动控制器。返回错误仅代表启动阶段失败。
func Run(c *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface, controllerInitializers map[string]InitFunc, webhooks map[string]WebhookHandler,
	stopCh <-chan struct{}) error {
	// To help debugging, immediately log version
	klog.Infof("Version: %+v", version.Get())

	// Start events processing pipeline.
	c.EventBroadcaster.StartStructuredLogging(0)
	c.EventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.Client.CoreV1().Events("")})
	defer c.EventBroadcaster.Shutdown()

	// /configz 输出转换后的 v1alpha1 视图，并显式设置 GVK，避免 kubectl 无法识别。
	externalConfig := &configv1alpha.CloudControllerManagerConfiguration{}
	if err := configv1alpha.Convert_config_CloudControllerManagerConfiguration_To_v1alpha1_CloudControllerManagerConfiguration(&c.ComponentConfig, externalConfig, nil); err != nil {
		return fmt.Errorf("unable to convert configz: %w", err)
	}
	externalConfig.SetGroupVersionKind(configv1alpha.SchemeGroupVersion.WithKind("CloudControllerManagerConfiguration"))

	// setup /configz endpoint
	if cz, err := configz.New(ConfigzName); err != nil {
		return fmt.Errorf("unable to register configz: %w", err)
	} else if err := cz.Set(externalConfig); err != nil {
		return fmt.Errorf("unable to set configz: %w", err)
	}

	// Setup any health checks we will want to use.
	// 选主健康检查在失去租约后 20 秒内失败，给探针足够时间反映状态。
	var checks []healthz.HealthChecker
	var electionChecker *leaderelection.HealthzAdaptor
	if c.ComponentConfig.Generic.LeaderElection.LeaderElect {
		electionChecker = leaderelection.NewLeaderHealthzAdaptor(time.Second * 20)
		checks = append(checks, electionChecker)
	}

	// webhook 是可选特性，只有门控开启且确有处理器时才启动服务端。
	if utilfeature.DefaultFeatureGate.Enabled(cmfeatures.CloudControllerManagerWebhook) {
		if len(webhooks) > 0 {
			klog.Info("Webhook Handlers enabled: ", webhooks)
			handler := newHandler(webhooks)
			if _, _, err := c.WebhookSecureServing.Serve(handler, 0, stopCh); err != nil {
				return err
			}
		}
	}

	healthzHandler := controllerhealthz.NewMutableHealthzHandler(checks...)
	// Start the controller manager HTTP server
	// 安全服务端承载 /healthz、/metrics 等；未配置时（测试场景）跳过。
	if c.SecureServing != nil {
		unsecuredMux := genericcontrollermanager.NewBaseHandler(&c.ComponentConfig.Generic.Debugging, healthzHandler)

		slis.SLIMetricsWithReset{}.Install(unsecuredMux)

		handler := genericcontrollermanager.BuildHandlerChain(unsecuredMux, &c.Authorization, &c.Authentication)
		// TODO: handle stoppedCh and listenerStoppedCh returned by c.SecureServing.Serve
		if _, _, err := c.SecureServing.Serve(handler, 0, stopCh); err != nil {
			return err
		}
	}

	// run 是实际启动控制器的闭包；选主与未选主两条路径共用，保证行为一致。
	run := func(ctx context.Context, controllerInitializers map[string]InitFunc) {
		clientBuilder := clientbuilder.SimpleControllerClientBuilder{
			ClientConfig: c.Kubeconfig,
		}
		controllerContext, err := CreateControllerContext(c, clientBuilder, ctx.Done())
		if err != nil {
			klog.Fatalf("error building controller context: %v", err)
		}
		if err := startControllers(ctx, cloud, controllerContext, c, ctx.Done(), controllerInitializers, healthzHandler); err != nil {
			klog.Fatalf("error running controllers: %v", err)
		}
	}

	// 未启用选主时直接运行，随后阻塞到 stopCh 关闭。
	if !c.ComponentConfig.Generic.LeaderElection.LeaderElect {
		ctx := wait.ContextForChannel(stopCh)
		run(ctx, controllerInitializers)
		<-stopCh
		return nil
	}

	// Identity used to distinguish between multiple cloud controller manager instances
	// hostname + UUID 保证同机多进程也能区分，避免误判为同一候选者。
	id, err := os.Hostname()
	if err != nil {
		return err
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id = id + "_" + string(uuid.NewUUID())

	// leaderMigrator will be non-nil if and only if Leader Migration is enabled.
	var leaderMigrator *leadermigration.LeaderMigrator = nil

	// If leader migration is enabled, use the redirected initialization
	// Check feature gate and configuration separately so that any error in configuration checking will not
	//  affect the result if the feature is not enabled.
	if leadermigration.Enabled(&c.ComponentConfig.Generic) {
		klog.Info("starting leader migration")

		leaderMigrator = leadermigration.NewLeaderMigrator(&c.ComponentConfig.Generic.LeaderMigration,
			"cloud-controller-manager")
	}

	// Start the main lock
	// 主锁只运行“未迁移”控制器；迁移锁由旧版本持有，二者通过 migration-ready 信号协调。
	go leaderElectAndRun(c, id, electionChecker,
		c.ComponentConfig.Generic.LeaderElection.ResourceLock,
		c.ComponentConfig.Generic.LeaderElection.ResourceName,
		leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				initializers := controllerInitializers
				if leaderMigrator != nil {
					// If leader migration is enabled, we should start only non-migrated controllers
					//  for the main lock.
					initializers = filterInitializers(controllerInitializers, leaderMigrator.FilterFunc, leadermigration.ControllerNonMigrated)
					klog.Info("leader migration: starting main controllers.")
					// Signal the main lock is acquired, and thus migration lock is ready to attempt.
					close(leaderMigrator.MigrationReady)
				}
				run(ctx, initializers)
			},
			OnStoppedLeading: func() {
				// 失去租约后必须退出，防止两个实例同时操作云资源。
				klog.ErrorS(nil, "leaderelection lost")
				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
			},
		})

	// If Leader Migration is enabled, proceed to attempt the migration lock.
	if leaderMigrator != nil {
		// Wait for the signal of main lock being acquired.
		<-leaderMigrator.MigrationReady

		// Start the migration lock.
		go leaderElectAndRun(c, id, electionChecker,
			c.ComponentConfig.Generic.LeaderMigration.ResourceLock,
			c.ComponentConfig.Generic.LeaderMigration.LeaderName,
			leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					klog.Info("leader migration: starting migrated controllers.")
					run(ctx, filterInitializers(controllerInitializers, leaderMigrator.FilterFunc, leadermigration.ControllerMigrated))
				},
				OnStoppedLeading: func() {
					klog.ErrorS(nil, "migration leaderelection lost")
					klog.FlushAndExit(klog.ExitFlushTimeout, 1)
				},
			})
	}

	<-stopCh
	return nil
}

// startControllers starts the cloud specific controller loops.
// 先让云实现完成 Initialize 并注入 informer，再依次启动已启用的控制器；
// 每个控制器启动后短暂抖动，最后等待 informer 缓存同步并阻塞直到停机。
func startControllers(ctx context.Context, cloud cloudprovider.Interface, controllerContext genericcontrollermanager.ControllerContext, c *cloudcontrollerconfig.CompletedConfig, stopCh <-chan struct{}, controllers map[string]InitFunc, healthzHandler *controllerhealthz.MutableHealthzHandler) error {
	// Initialize the cloud provider with a reference to the clientBuilder
	cloud.Initialize(c.ClientBuilder, stopCh)
	// Set the informer on the user cloud object
	// 仅部分云实现需要共享 informer；类型断言失败时静默跳过。
	if informerUserCloud, ok := cloud.(cloudprovider.InformerUser); ok {
		informerUserCloud.SetInformers(c.SharedInformers)
	}
	var controllerChecks []healthz.HealthChecker
	for controllerName, initFn := range controllers {
		if !genericcontrollermanager.IsControllerEnabled(controllerName, ControllersDisabledByDefault, c.ComponentConfig.Generic.Controllers) {
			klog.Warningf("%q is disabled", controllerName)
			continue
		}

		klog.V(1).Infof("Starting %q", controllerName)
		ctrl, started, err := initFn(ctx, controllerContext)
		if err != nil {
			klog.Errorf("Error starting %q", controllerName)
			return err
		}
		if !started {
			klog.Warningf("Skipping %q", controllerName)
			continue
		}
		// 默认用 ping checker；控制器若实现 HealthCheckable 则使用其真实健康检查。
		check := controllerhealthz.NamedPingChecker(controllerName)
		if ctrl != nil {
			if healthCheckable, ok := ctrl.(controller.HealthCheckable); ok {
				if realCheck := healthCheckable.HealthChecker(); realCheck != nil {
					check = controllerhealthz.NamedHealthChecker(controllerName, realCheck)
				}
			}
		}
		controllerChecks = append(controllerChecks, check)
		klog.Infof("Started %q", controllerName)

		time.Sleep(wait.Jitter(c.ComponentConfig.Generic.ControllerStartInterval.Duration, ControllerStartJitter))
	}

	healthzHandler.AddHealthChecker(controllerChecks...)

	// If apiserver is not running we should wait for some time and fail only then. This is particularly
	// important when we start apiserver and controller manager at the same time.
	if err := genericcontrollermanager.WaitForAPIServer(c.VersionedClient, 10*time.Second); err != nil {
		klog.Fatalf("Failed to wait for apiserver being healthy: %v", err)
	}

	// informer 必须在控制器已注册事件处理器之后再启动，否则可能遗漏初始事件。
	c.SharedInformers.Start(stopCh)
	controllerContext.InformerFactory.Start(controllerContext.Stop)

	<-stopCh
	return nil
}

// InitCloudFunc is used to initialize cloud
// 由厂商注入：根据完整配置构造 cloudprovider.Interface。返回 nil 会导致后续 panic。
type InitCloudFunc func(config *cloudcontrollerconfig.CompletedConfig) cloudprovider.Interface

// InitFunc is used to launch a particular controller. It returns a controller
// that can optionally implement other interfaces so that the controller manager
// can support the requested features.
// The returned controller may be nil, which will be considered an anonymous controller
// that requests no additional features from the controller manager.
// Any error returned will cause the controller process to `Fatal`
// The bool indicates whether the controller was enabled.
// 返回 enabled=false 表示前置条件不满足（如云不支持该能力），属于正常跳过而非错误。
type InitFunc func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller controller.Interface, enabled bool, err error)

// InitFuncConstructor is used to construct InitFunc
// 把厂商无关的配置与云对象绑定到具体控制器，生成可被 startControllers 调用的 InitFunc。
type InitFuncConstructor func(initcontext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc

// ControllerNames indicate the default controller we are known.
// 排序后返回，保证 flag/help 输出稳定。
func ControllerNames(controllerInitFuncConstructors map[string]ControllerInitFuncConstructor) []string {
	ret := sets.StringKeySet(controllerInitFuncConstructors)
	return ret.List()
}

var (
	// ControllersDisabledByDefault is the controller disabled default when starting cloud-controller managers.
	// 当前为空：所有注册的 CCM 控制器默认启用，与历史行为保持一致。
	ControllersDisabledByDefault = sets.NewString()

	// AllWebhooks represents the list of all webhook options configured in
	// this package.  This is empty because no webhooks are currently
	// configured in this package.
	AllWebhooks = []string{}

	// DisabledByDefaultWebhooks represents the list of webhooks which must be
	// explicitly enabled. This is empty because no webhooks are currently
	// configured in this package.
	DisabledByDefaultWebhooks = []string{}
)

// ConstructControllerInitializers is a map of controller name(as defined by controllers flag in https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/#options) to their InitFuncConstructor.
// paired to their InitFunc.  This allows for structured downstream composition and subdivision.
// 在此一次性完成所有构造，使启动阶段只做简单调用，便于错误集中暴露。
func ConstructControllerInitializers(controllerInitFuncConstructors map[string]ControllerInitFuncConstructor, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) map[string]InitFunc {
	controllers := map[string]InitFunc{}
	for name, constructor := range controllerInitFuncConstructors {
		controllers[name] = constructor.Constructor(constructor.InitContext, completedConfig, cloud)
	}
	return controllers
}

// ControllerInitFuncConstructor 把一个控制器的构造器与其运行上下文绑定；
// InitContext 决定该控制器使用的客户端身份。
type ControllerInitFuncConstructor struct {
	InitContext ControllerInitContext
	Constructor InitFuncConstructor
}

// ControllerInitContext 携带控制器启动时需要的静态上下文。
// ClientName 用于从 clientBuilder 获取对应 ServiceAccount 的客户端。
type ControllerInitContext struct {
	ClientName string
}

// StartCloudNodeControllerWrapper is used to take cloud config as input and start cloud node controller
// 包装成 InitFunc，延迟到选主成功后才真正创建客户端与控制器。
func StartCloudNodeControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startCloudNodeController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// StartCloudNodeLifecycleControllerWrapper is used to take cloud config as input and start cloud node lifecycle controller
func StartCloudNodeLifecycleControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startCloudNodeLifecycleController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// StartServiceControllerWrapper is used to take cloud config as input and start service controller
func StartServiceControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startServiceController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// StartRouteControllerWrapper is used to take cloud config as input and start route controller
func StartRouteControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startRouteController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// DefaultInitFuncConstructors is a map of default named controller groups paired with InitFuncConstructor
// 名称常量来自 names 包，避免字符串散落导致 flag 与实现不一致。
var DefaultInitFuncConstructors = map[string]ControllerInitFuncConstructor{
	// The cloud-node controller shares the "node-controller" identity with the cloud-node-lifecycle
	// controller for historical reasons.  See
	// https://github.com/kubernetes/kubernetes/pull/72764#issuecomment-453300990 for more context.
	// 共享客户端身份是为了兼容既有 RBAC 绑定，新控制器不应继续沿用这一做法。
	names.CloudNodeController: {
		InitContext: ControllerInitContext{
			ClientName: "node-controller",
		},
		Constructor: StartCloudNodeControllerWrapper,
	},
	names.CloudNodeLifecycleController: {
		InitContext: ControllerInitContext{
			ClientName: "node-controller",
		},
		Constructor: StartCloudNodeLifecycleControllerWrapper,
	},
	names.ServiceLBController: {
		InitContext: ControllerInitContext{
			ClientName: "service-controller",
		},
		Constructor: StartServiceControllerWrapper,
	},
	names.NodeRouteController: {
		InitContext: ControllerInitContext{
			ClientName: "route-controller",
		},
		Constructor: StartRouteControllerWrapper,
	},
}

// CreateControllerContext creates a context struct containing references to resources needed by the
// controllers such as the cloud provider and clientBuilder. rootClientBuilder is only used for
// the shared-informers client and token controller.
// 这里同时创建共享 informer、metadata informer 与可刷新的 discovery RESTMapper。
func CreateControllerContext(s *cloudcontrollerconfig.CompletedConfig, clientBuilder clientbuilder.ControllerClientBuilder, stop <-chan struct{}) (genericcontrollermanager.ControllerContext, error) {
	versionedClient := clientBuilder.ClientOrDie("shared-informers")
	// 每个控制器工厂使用独立的 resync 周期函数，避免所有 informer 同步触发。
	sharedInformers := informers.NewSharedInformerFactory(versionedClient, ResyncPeriod(s)())

	metadataClient := metadata.NewForConfigOrDie(clientBuilder.ConfigOrDie("metadata-informers"))
	metadataInformers := metadatainformer.NewSharedInformerFactory(metadataClient, ResyncPeriod(s)())

	// If apiserver is not running we should wait for some time and fail only then. This is particularly
	// important when we start apiserver and controller manager at the same time.
	if err := genericcontrollermanager.WaitForAPIServer(versionedClient, 10*time.Second); err != nil {
		return genericcontrollermanager.ControllerContext{}, fmt.Errorf("failed to wait for apiserver being healthy: %v", err)
	}

	// Use a discovery client capable of being refreshed.
	// discovery 结果会随 CRD 等资源变化而失效，因此定期 Reset 延迟初始化的 RESTMapper。
	discoveryClient := clientBuilder.ClientOrDie("controller-discovery")
	cachedClient := cacheddiscovery.NewMemCacheClient(discoveryClient.Discovery())
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedClient)
	go wait.Until(func() {
		restMapper.Reset()
	}, 30*time.Second, stop)

	ctx := genericcontrollermanager.ControllerContext{
		ClientBuilder:                   clientBuilder,
		InformerFactory:                 sharedInformers,
		ObjectOrMetadataInformerFactory: informerfactory.NewInformerFactory(sharedInformers, metadataInformers),
		RESTMapper:                      restMapper,
		Stop:                            stop,
		InformersStarted:                make(chan struct{}),
		ResyncPeriod:                    ResyncPeriod(s),
		ControllerManagerMetrics:        controllersmetrics.NewControllerManagerMetrics("cloud-controller-manager"),
	}
	controllersmetrics.Register()
	return ctx, nil
}

// ResyncPeriod returns a function which generates a duration each time it is
// invoked; this is because that multiple controllers don't get into lock-step.
// 在 [1,2) 倍 MinResyncPeriod 之间随机取值，让各 informer 的周期同步错开。
func ResyncPeriod(c *cloudcontrollerconfig.CompletedConfig) func() time.Duration {
	return func() time.Duration {
		factor := rand.Float64() + 1
		return time.Duration(float64(c.ComponentConfig.Generic.MinResyncPeriod.Nanoseconds()) * factor)
	}
}

// leaderElectAndRun runs the leader election, and runs the callbacks once the leader lease is acquired.
// TODO: extract this function into staging/controller-manager
// 租约使用独立 kubeconfig；获得租约后回调内启动控制器，永久运行直到进程退出。
func leaderElectAndRun(c *cloudcontrollerconfig.CompletedConfig, lockIdentity string, electionChecker *leaderelection.HealthzAdaptor, resourceLock string, leaseName string, callbacks leaderelection.LeaderCallbacks) {
	rl, err := resourcelock.NewFromKubeconfig(resourceLock,
		c.ComponentConfig.Generic.LeaderElection.ResourceNamespace,
		leaseName,
		resourcelock.ResourceLockConfig{
			Identity:      lockIdentity,
			EventRecorder: c.EventRecorder,
		},
		c.Kubeconfig,
		c.ComponentConfig.Generic.LeaderElection.RenewDeadline.Duration)
	if err != nil {
		klog.Fatalf("error creating lock: %v", err)
	}

	leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: c.ComponentConfig.Generic.LeaderElection.LeaseDuration.Duration,
		RenewDeadline: c.ComponentConfig.Generic.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:   c.ComponentConfig.Generic.LeaderElection.RetryPeriod.Duration,
		Callbacks:     callbacks,
		WatchDog:      electionChecker,
		Name:          leaseName,
	})

	panic("unreachable")
}

// filterInitializers returns initializers that has filterFunc(name) == expected.
// filterFunc can be nil, in which case the original initializers will be returned directly.
// InitFunc is local to cloud-controller-manager, and thus filterInitializers has to be local too.
// 迁移场景下按“已迁移/未迁移”把控制器分给两把锁，避免同一控制器被启动两次。
func filterInitializers(allInitializers map[string]InitFunc, filterFunc leadermigration.FilterFunc, expected leadermigration.FilterResult) map[string]InitFunc {
	if filterFunc == nil {
		return allInitializers
	}
	initializers := make(map[string]InitFunc)
	for name, initFunc := range allInitializers {
		if filterFunc(name) == expected {
			initializers[name] = initFunc
		}
	}
	return initializers
}
