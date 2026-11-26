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
	// ControllerStartJitter 是启动各控制器时使用的抖动值。
	// 依次启动控制器时加入 1.0 抖动，避免所有控制器在重启瞬间同时打 API。
	ControllerStartJitter = 1.0
	// ConfigzName 是注册 cloud-controller manager /configz 时使用的名称，与 GroupName 相同。
	// 与配置 GroupName 相同，便于通过 /configz 直接识别组件类型。
	ConfigzName = "cloudcontrollermanager.config.k8s.io"
)

// NewCloudControllerManagerCommand 使用默认参数创建 *cobra.Command 对象。
// controllerInitFuncConstructors 是控制器名称（如 https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/#options 中 controllers flag 所定义）到其 InitFuncConstructor 的映射。
// additionalFlags 提供控制器专属 flag，并入控制器管理器的完整 flag 集合。
// 这是不经过 CommandBuilder 的传统入口；与 builder 路径的区别在于 webhook 集合固定为空。
func NewCloudControllerManagerCommand(s *options.CloudControllerManagerOptions, cloudInitializer InitCloudFunc, controllerInitFuncConstructors map[string]ControllerInitFuncConstructor, controllerAliases map[string]string, additionalFlags cliflag.NamedFlagSets, stopCh <-chan struct{}) *cobra.Command {
	logOptions := logs.NewOptions()

	cmd := &cobra.Command{
		Use: "cloud-controller-manager",
		Long: `The Cloud controller manager is a daemon that embeds
the cloud specific control loops shipped with Kubernetes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()

			// 尽快激活日志配置，之后再打印 flags，使输出的日志选项与实际运行时一致。
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

// Run 运行 ExternalCMServer，且不应退出。
// 职责：建立事件与 metrics 管道、注册 /configz 与健康检查、按需启动 webhook 与安全服务端，
// 最后在选主成功（或未启用选主）时启动控制器。返回错误仅代表启动阶段失败。
func Run(c *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface, controllerInitializers map[string]InitFunc, webhooks map[string]WebhookHandler,
	stopCh <-chan struct{}) error {
	// 启动时立即打印版本，方便排障。
	klog.Infof("Version: %+v", version.Get())

	// 启动事件处理管道。
	c.EventBroadcaster.StartStructuredLogging(0)
	c.EventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.Client.CoreV1().Events("")})
	defer c.EventBroadcaster.Shutdown()

	// /configz 输出转换后的 v1alpha1 视图，并显式设置 GVK，避免 kubectl 无法识别。
	externalConfig := &configv1alpha.CloudControllerManagerConfiguration{}
	if err := configv1alpha.Convert_config_CloudControllerManagerConfiguration_To_v1alpha1_CloudControllerManagerConfiguration(&c.ComponentConfig, externalConfig, nil); err != nil {
		return fmt.Errorf("unable to convert configz: %w", err)
	}
	externalConfig.SetGroupVersionKind(configv1alpha.SchemeGroupVersion.WithKind("CloudControllerManagerConfiguration"))

	// 注册 /configz 端点。
	if cz, err := configz.New(ConfigzName); err != nil {
		return fmt.Errorf("unable to register configz: %w", err)
	} else if err := cz.Set(externalConfig); err != nil {
		return fmt.Errorf("unable to set configz: %w", err)
	}

	// 设置后续要用到的健康检查。
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
	// 启动控制器管理器的 HTTP 服务器。
	// 安全服务端承载 /healthz、/metrics 等；未配置时（测试场景）跳过。
	if c.SecureServing != nil {
		unsecuredMux := genericcontrollermanager.NewBaseHandler(&c.ComponentConfig.Generic.Debugging, healthzHandler)

		slis.SLIMetricsWithReset{}.Install(unsecuredMux)

		handler := genericcontrollermanager.BuildHandlerChain(unsecuredMux, &c.Authorization, &c.Authentication)
		// TODO: 处理 c.SecureServing.Serve 返回的 stoppedCh 与 listenerStoppedCh。
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

	// 用于区分多个 cloud controller manager 实例的身份标识。
	// hostname + UUID 保证同机多进程也能区分，避免误判为同一候选者。
	id, err := os.Hostname()
	if err != nil {
		return err
	}
	// 加唯一后缀，避免同机多实例被误判为同一候选者。
	id = id + "_" + string(uuid.NewUUID())

	// 仅当启用 Leader Migration 时 leaderMigrator 才非空。
	var leaderMigrator *leadermigration.LeaderMigrator = nil

	// 启用 Leader Migration 时走重定向初始化路径。
	// feature gate 与配置分开检查，未启用该特性时配置错误不应影响结果。
	if leadermigration.Enabled(&c.ComponentConfig.Generic) {
		klog.Info("starting leader migration")

		leaderMigrator = leadermigration.NewLeaderMigrator(&c.ComponentConfig.Generic.LeaderMigration,
			"cloud-controller-manager")
	}

	// 启动主锁。
	// 主锁只运行“未迁移”控制器；迁移锁由旧版本持有，二者通过 migration-ready 信号协调。
	go leaderElectAndRun(c, id, electionChecker,
		c.ComponentConfig.Generic.LeaderElection.ResourceLock,
		c.ComponentConfig.Generic.LeaderElection.ResourceName,
		leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				initializers := controllerInitializers
				if leaderMigrator != nil {
					// 启用 Leader Migration 时主锁只启动未迁移的控制器。
					initializers = filterInitializers(controllerInitializers, leaderMigrator.FilterFunc, leadermigration.ControllerNonMigrated)
					klog.Info("leader migration: starting main controllers.")
					// 通知主锁已获取，可以开始争取迁移锁。
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

	// 启用 Leader Migration 后继续争取迁移锁。
	if leaderMigrator != nil {
		// 等待主锁获取成功的信号。
		<-leaderMigrator.MigrationReady

		// 启动迁移锁。
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

// startControllers 启动云厂商特定的控制器循环。
// 先让云实现完成 Initialize 并注入 informer，再依次启动已启用的控制器；
// 每个控制器启动后短暂抖动，最后等待 informer 缓存同步并阻塞直到停机。
func startControllers(ctx context.Context, cloud cloudprovider.Interface, controllerContext genericcontrollermanager.ControllerContext, c *cloudcontrollerconfig.CompletedConfig, stopCh <-chan struct{}, controllers map[string]InitFunc, healthzHandler *controllerhealthz.MutableHealthzHandler) error {
	// 用 clientBuilder 初始化云 provider，使其能获取客户端。
	cloud.Initialize(c.ClientBuilder, stopCh)
	// 为云对象设置 informer。
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

	// apiserver 可能启动较慢，等待一段时间再判定失败；同时启动 apiserver 与控制器时需要这段缓冲。
	if err := genericcontrollermanager.WaitForAPIServer(c.VersionedClient, 10*time.Second); err != nil {
		klog.Fatalf("Failed to wait for apiserver being healthy: %v", err)
	}

	// informer 必须在控制器已注册事件处理器之后再启动，否则可能遗漏初始事件。
	c.SharedInformers.Start(stopCh)
	controllerContext.InformerFactory.Start(controllerContext.Stop)

	<-stopCh
	return nil
}

// InitCloudFunc 用于初始化 cloud provider。
// 由厂商注入：根据完整配置构造 cloudprovider.Interface。返回 nil 会导致后续 panic。
type InitCloudFunc func(config *cloudcontrollerconfig.CompletedConfig) cloudprovider.Interface

// InitFunc 用于启动特定控制器。它返回一个 controller，该 controller 可以选择实现其他接口，
// 以便控制器管理器支持所请求的功能。
// 返回的 controller 可以为 nil，此时视为匿名控制器，不向控制器管理器请求额外功能。
// 返回任何错误都会导致控制器进程 `Fatal`。
// 返回的 bool 表示控制器是否被启用。
// 返回 enabled=false 表示前置条件不满足（如云不支持该能力），属于正常跳过而非错误。
type InitFunc func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller controller.Interface, enabled bool, err error)

// InitFuncConstructor 用于构造 InitFunc。
// 把厂商无关的配置与云对象绑定到具体控制器，生成可被 startControllers 调用的 InitFunc。
type InitFuncConstructor func(initcontext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc

// ControllerNames 返回我们已知的默认控制器列表。
// 排序后返回，保证 flag/help 输出稳定。
func ControllerNames(controllerInitFuncConstructors map[string]ControllerInitFuncConstructor) []string {
	ret := sets.StringKeySet(controllerInitFuncConstructors)
	return ret.List()
}

var (
	// ControllersDisabledByDefault 是启动 cloud-controller manager 时默认禁用的控制器集合。
	// 当前为空：所有注册的 CCM 控制器默认启用，与历史行为保持一致。
	ControllersDisabledByDefault = sets.NewString()

	// AllWebhooks 是本包配置的所有 webhook 列表；当前本包没有配置任何 webhook，故为空。
	AllWebhooks = []string{}

	// DisabledByDefaultWebhooks 是必须显式启用的 webhook 列表；当前没有配置任何 webhook，故为空。
	DisabledByDefaultWebhooks = []string{}
)

// ConstructControllerInitializers 将控制器名称（如 https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/#options 中 controllers flag 所定义）到
// 其 InitFuncConstructor 的映射转换为名称到 InitFunc 的映射，便于下游结构化地组合与拆分。
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

// StartCloudNodeControllerWrapper 用于接收云配置作为输入并启动 cloud node controller。
// 包装成 InitFunc，延迟到选主成功后才真正创建客户端与控制器。
func StartCloudNodeControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startCloudNodeController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// StartCloudNodeLifecycleControllerWrapper 接收云配置并启动 cloud node lifecycle controller。
func StartCloudNodeLifecycleControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startCloudNodeLifecycleController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// StartServiceControllerWrapper 接收云配置并启动 Service 控制器。
func StartServiceControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startServiceController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// StartRouteControllerWrapper 接收云配置并启动路由控制器。
func StartRouteControllerWrapper(initContext ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startRouteController(ctx, initContext, controllerContext, completedConfig, cloud)
	}
}

// DefaultInitFuncConstructors 是默认命名的控制器组到 InitFuncConstructor 的映射。
// 名称常量来自 names 包，避免字符串散落导致 flag 与实现不一致。
var DefaultInitFuncConstructors = map[string]ControllerInitFuncConstructor{
	// 出于历史原因，cloud-node 控制器与 cloud-node-lifecycle 控制器共享 "node-controller" 身份，参见
	// https://github.com/kubernetes/kubernetes/pull/72764#issuecomment-453300990 了解更多背景。
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

// CreateControllerContext 创建一个上下文结构，包含控制器所需资源的引用，例如云 provider 和 clientBuilder。
// rootClientBuilder 仅用于共享 informer 的客户端和 token 控制器。
// 这里同时创建共享 informer、metadata informer 与可刷新的 discovery RESTMapper。
func CreateControllerContext(s *cloudcontrollerconfig.CompletedConfig, clientBuilder clientbuilder.ControllerClientBuilder, stop <-chan struct{}) (genericcontrollermanager.ControllerContext, error) {
	versionedClient := clientBuilder.ClientOrDie("shared-informers")
	// 每个控制器工厂使用独立的 resync 周期函数，避免所有 informer 同步触发。
	sharedInformers := informers.NewSharedInformerFactory(versionedClient, ResyncPeriod(s)())

	metadataClient := metadata.NewForConfigOrDie(clientBuilder.ConfigOrDie("metadata-informers"))
	metadataInformers := metadatainformer.NewSharedInformerFactory(metadataClient, ResyncPeriod(s)())

	// apiserver 可能启动较慢，等待一段时间再判定失败；同时启动 apiserver 与控制器时需要这段缓冲。
	if err := genericcontrollermanager.WaitForAPIServer(versionedClient, 10*time.Second); err != nil {
		return genericcontrollermanager.ControllerContext{}, fmt.Errorf("failed to wait for apiserver being healthy: %v", err)
	}

	// 使用可刷新的 discovery 客户端。
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

// ResyncPeriod 返回一个每次调用都生成新时长的函数；这是为了避免多个控制器陷入锁步同步。
// 在 [1,2) 倍 MinResyncPeriod 之间随机取值，让各 informer 的周期同步错开。
func ResyncPeriod(c *cloudcontrollerconfig.CompletedConfig) func() time.Duration {
	return func() time.Duration {
		factor := rand.Float64() + 1
		return time.Duration(float64(c.ComponentConfig.Generic.MinResyncPeriod.Nanoseconds()) * factor)
	}
}

// leaderElectAndRun 运行 leader 选举，并在获得 leader 租约后运行回调。
// TODO: 将此函数提取到 staging/controller-manager。
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

// filterInitializers 返回满足 filterFunc(name) == expected 的 initializers。
// filterFunc 可以为 nil，此时直接返回原始 initializers。
// InitFunc 是 cloud-controller-manager 内部类型，因此 filterInitializers 也必须留在本地。
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
