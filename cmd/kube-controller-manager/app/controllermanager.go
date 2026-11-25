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

// Package app 实现控制器管理服务器，运行复制控制器、服务端点和节点等活跃组件。
package app

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/spf13/cobra"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/statusz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	certutil "k8s.io/client-go/util/cert"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	metricsfeatures "k8s.io/component-base/metrics/features"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/component-base/term"
	utilversion "k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	genericcontrollermanager "k8s.io/controller-manager/app"
	"k8s.io/controller-manager/controller"
	"k8s.io/controller-manager/pkg/clientbuilder"
	cmfeatures "k8s.io/controller-manager/pkg/features"
	controllerhealthz "k8s.io/controller-manager/pkg/healthz"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/controller-manager/pkg/leadermigration"
	"k8s.io/klog/v2"
	configv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app/config"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app/options"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	kubectrlmgrconfig "k8s.io/kubernetes/pkg/controller/apis/config"
	configv1alpha1conversion "k8s.io/kubernetes/pkg/controller/apis/config/v1alpha1"
	garbagecollector "k8s.io/kubernetes/pkg/controller/garbagecollector"
	kubefeatures "k8s.io/kubernetes/pkg/features"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	utilruntime.Must(metricsfeatures.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

const (
	// ControllerStartJitter 是控制器启动时随机延迟的最大倍率。
	ControllerStartJitter = 1.0
	// ConfigzName 是控制器管理器在 /configz 注册的配置名称，与 GroupName 一致。
	ConfigzName = "kubecontrollermanager.config.k8s.io"
	// kubeControllerManager 是内部使用的控制器管理器组件名称。
	kubeControllerManager = "kube-controller-manager"
)

// NewControllerManagerCommand 创建采用默认配置的控制器管理器命令。
func NewControllerManagerCommand() *cobra.Command {
	s, err := options.NewKubeControllerManagerOptions()
	if err != nil {
		klog.Background().Error(err, "Unable to initialize command options")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	cmd := &cobra.Command{
		Use: kubeControllerManager,
		Long: `The Kubernetes controller manager is a daemon that embeds
the core control loops shipped with Kubernetes. In applications of robotics and
automation, a control loop is a non-terminating loop that regulates the state of
the system. In Kubernetes, a controller is a control loop that watches the shared
state of the cluster through the apiserver and makes changes attempting to move the
current state towards the desired state. Examples of controllers that ship with
Kubernetes today are the replication controller, endpoints controller, namespace
controller, and serviceaccounts controller.`,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			// 管理器会监听包括已弃用版本在内的 API；CI 已验证匹配版本兼容性，因此屏蔽客户端警告。
			restclient.SetDefaultWarningHandler(restclient.NoWarnings{})
			// 确保在 RunE 前应用特性门控。
			return s.ComponentGlobalsRegistry.Set()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()

			// 尽早应用日志配置，随后按最终配置打印参数。
			if err := logsapi.ValidateAndApply(s.Logs, utilfeature.DefaultFeatureGate); err != nil {
				return err
			}
			cliflag.PrintFlags(cmd.Flags())

			// 此处仍使用 context.Background()，避免信号到达时事件广播器等组件过早终止；关闭流程由 Run 显式处理。
			ctx := context.Background()
			c, err := s.Config(ctx, KnownControllers(), ControllersDisabledByDefault(), ControllerAliases())
			if err != nil {
				return err
			}

			// 添加特性启用状态指标
			fg := s.ComponentGlobalsRegistry.FeatureGateFor(basecompatibility.DefaultKubeComponent)
			fg.(featuregate.MutableFeatureGate).AddMetrics()
			// 添加组件版本指标
			s.ComponentGlobalsRegistry.AddMetrics()

			if utilfeature.DefaultFeatureGate.Enabled(cmfeatures.ControllerManagerReleaseLeaderElectionLockOnExit) {
				ctx = server.SetupSignalContext()
			}
			return Run(ctx, c.Complete())
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	fs := cmd.Flags()
	namedFlagSets := s.Flags(KnownControllers(), ControllersDisabledByDefault(), ControllerAliases())
	s.ParsedFlags = &namedFlagSets
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, namedFlagSets, cols)

	return cmd
}

// ResyncPeriod 返回带随机抖动的时间生成函数，避免多个控制器同步发起 list 请求。
func ResyncPeriod(c *config.CompletedConfig) func() time.Duration {
	return func() time.Duration {
		factor := rand.Float64() + 1
		return time.Duration(float64(c.ComponentConfig.Generic.MinResyncPeriod.Nanoseconds()) * factor)
	}
}

// Run 按完成态配置启动控制器管理器，并阻塞至关闭。
func Run(ctx context.Context, c *config.CompletedConfig) error {
	logger := klog.FromContext(ctx)
	stopCh := ctx.Done()

	// 立即记录版本，便于排查问题。
	logger.Info("Starting", "version", utilversion.Get())

	logger.Info("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	// 启动事件处理和记录流水线。
	c.EventBroadcaster.StartStructuredLogging(0)
	c.EventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.Client.CoreV1().Events("")})
	defer c.EventBroadcaster.Shutdown()

	externalConfig := &configv1alpha1.KubeControllerManagerConfiguration{}
	if err := configv1alpha1conversion.Convert_config_KubeControllerManagerConfiguration_To_v1alpha1_KubeControllerManagerConfiguration(&c.ComponentConfig, externalConfig, nil); err != nil {
		return fmt.Errorf("unable to convert configz: %w", err)
	}
	externalConfig.SetGroupVersionKind(configv1alpha1.SchemeGroupVersion.WithKind("KubeControllerManagerConfiguration"))

	if cfgz, err := configz.New(ConfigzName); err != nil {
		return fmt.Errorf("unable to register configz: %w", err)
	} else if err := cfgz.Set(externalConfig); err != nil {
		return fmt.Errorf("unable to set configz: %w", err)
	}

	// 注册健康检查。
	var checks []healthz.HealthChecker
	var electionChecker *leaderelection.HealthzAdaptor
	if c.ComponentConfig.Generic.LeaderElection.LeaderElect {
		electionChecker = leaderelection.NewLeaderHealthzAdaptor(time.Second * 20)
		checks = append(checks, electionChecker)
	}
	healthzHandler := controllerhealthz.NewMutableHealthzHandler(checks...)

	// 启动管理器 HTTP 服务；unsecuredMux 中的处理器仍需经过认证和授权过滤器。
	var unsecuredMux *mux.PathRecorderMux
	if c.SecureServing != nil {
		unsecuredMux = genericcontrollermanager.NewBaseHandler(&c.ComponentConfig.Generic.Debugging, healthzHandler)
		slis.SLIMetricsWithReset{}.Install(unsecuredMux)
		if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
			if c.Flagz != nil {
				flagz.Install(unsecuredMux, kubeControllerManager, c.Flagz)
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
			statusz.Install(
				unsecuredMux,
				kubeControllerManager,
				statusz.NewRegistry(
					c.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent),
					statusz.WithListedPaths(unsecuredMux.ListedPaths()),
				),
			)
		}

		handler := genericcontrollermanager.BuildHandlerChain(unsecuredMux, &c.Authorization, &c.Authentication)
		// TODO：处理 c.SecureServing.Serve 返回的 stoppedCh 和 listenerStoppedCh。
		if _, _, err := c.SecureServing.Serve(handler, 0, stopCh); err != nil {
			return err
		}
	}

	clientBuilder, rootClientBuilder := createClientBuilders(c)

	saTokenControllerDescriptor := newServiceAccountTokenControllerDescriptor(rootClientBuilder)

	run := func(ctx context.Context, controllerDescriptors map[string]*ControllerDescriptor) error {
		controllerContext, err := CreateControllerContext(ctx, c, rootClientBuilder, clientBuilder)
		if err != nil {
			logger.Error(err, "Error building controller context")
			return err
		}

		// 提前构造全部控制器，避免启动后部分初始化失败。
		controllers, err := BuildControllers(ctx, controllerContext, controllerDescriptors, unsecuredMux, healthzHandler)
		if err != nil {
			logger.Error(err, "Error building controllers")
			return err
		}

		// 启动共享 informer。
		stopCh := ctx.Done()
		controllerContext.InformerFactory.Start(stopCh)
		defer controllerContext.InformerFactory.Shutdown()
		controllerContext.ObjectOrMetadataInformerFactory.Start(stopCh)
		close(controllerContext.InformersStarted)

		// 正式启动控制器。
		if len(controllers) > 0 {
			if !RunControllers(ctx, controllerContext, controllers, ControllerStartJitter, c.ControllerShutdownTimeout) {
				return errors.New("controller shutdown timeout reached")
			}
		} else {
			<-ctx.Done()
		}
		return nil
	}

	// 未启用领导者选举时直接运行。
	if !c.ComponentConfig.Generic.LeaderElection.LeaderElect {
		controllerDescriptors := NewControllerDescriptors()
		controllerDescriptors[names.ServiceAccountTokenController] = saTokenControllerDescriptor
		return run(ctx, controllerDescriptors)
	}

	id, err := os.Hostname()
	if err != nil {
		return err
	}

	// 添加唯一后缀，避免同主机上的两个进程被误认为同一实例。
	id = id + "_" + string(uuid.NewUUID())

	// leaderMigrator 仅在启用领导者迁移时非空。
	var leaderMigrator *leadermigration.LeaderMigrator = nil

	// 启用领导者迁移时创建迁移器并完成准备。
	if leadermigration.Enabled(&c.ComponentConfig.Generic) {
		logger.Info("starting leader migration")

		leaderMigrator = leadermigration.NewLeaderMigrator(&c.ComponentConfig.Generic.LeaderMigration,
			kubeControllerManager)

		// startSATokenControllerInit 是原始构造器。
		saTokenControllerInit := saTokenControllerDescriptor.GetControllerConstructor()

		// 包装服务账号令牌控制器，在启动后通知迁移器已就绪。
		saTokenControllerDescriptor.constructor = func(ctx context.Context, controllerContext ControllerContext, controllerName string) (Controller, error) {
			ctrl, err := saTokenControllerInit(ctx, controllerContext, controllerName)
			if err != nil {
				return nil, err
			}

			// RunControllers 依赖类型断言，包装后会丢失原类型；此控制器不依赖该断言，因此不受影响。
			return newControllerLoop(func(ctx context.Context) {
				close(leaderMigrator.MigrationReady)
				ctrl.Run(ctx)
			}, controllerName), nil
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CoordinatedLeaderElection) {
		binaryVersion, err := semver.ParseTolerant(c.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent).BinaryVersion().String())
		if err != nil {
			return err
		}
		emulationVersion, err := semver.ParseTolerant(c.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent).EmulationVersion().String())
		if err != nil {
			return err
		}

		// 启动租约候选控制器以协调领导者选举。
		leaseCandidate, waitForSync, err := leaderelection.NewCandidate(
			c.Client,
			"kube-system",
			id,
			kubeControllerManager,
			binaryVersion.FinalizeVersion(),
			emulationVersion.FinalizeVersion(),
			coordinationv1.OldestEmulationVersion,
		)
		if err != nil {
			return err
		}
		healthzHandler.AddHealthChecker(healthz.NewInformerSyncHealthz(waitForSync))

		go leaseCandidate.Run(ctx)
	}

	// 启动主锁竞争。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// startedLeading 用于包装所有 OnStartedLeading 回调，以统一取消上下文并收集错误。
	var (
		errs     []error
		errsLock sync.Mutex
	)
	startedLeading := func(next func(context.Context) error) func(context.Context) {
		return func(ctx context.Context) {
			// 在 OnStartedLeading 结束时直接取消上下文，比等待释放锁后才触发 OnStoppedLeading 更及时。
			defer cancel()
			if err := next(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errsLock.Lock()
				errs = append(errs, err)
				errsLock.Unlock()
			}
		}
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		leaderElectAndRun(ctx, c, id, electionChecker,
			c.ComponentConfig.Generic.LeaderElection.ResourceLock,
			c.ComponentConfig.Generic.LeaderElection.ResourceName,
			leaderelection.LeaderCallbacks{
				OnStartedLeading: startedLeading(func(ctx context.Context) error {
					controllerDescriptors := NewControllerDescriptors()
					if leaderMigrator != nil {
						// 启用迁移时，主锁仅运行尚未迁移的控制器。
						controllerDescriptors = filteredControllerDescriptors(controllerDescriptors, leaderMigrator.FilterFunc, leadermigration.ControllerNonMigrated)
						logger.Info("leader migration: starting main controllers.")
					}
					controllerDescriptors[names.ServiceAccountTokenController] = saTokenControllerDescriptor
					return run(ctx, controllerDescriptors)
				}),
				OnStoppedLeading: func() {
					logger.Error(nil, "leaderelection lost/stopped")
					if !utilfeature.DefaultFeatureGate.Enabled(cmfeatures.ControllerManagerReleaseLeaderElectionLockOnExit) {
						klog.FlushAndExit(klog.ExitFlushTimeout, 1)
					}
				},
			})
	})

	// 启用领导者迁移时，继续竞争迁移锁。
	if leaderMigrator != nil {
		// 必须先取得主锁再竞争迁移锁，否则两个实例可能分别持有主锁和迁移锁。
		// 等服务账号令牌控制器启动后再获取迁移锁；此时主锁应已取得，或进程已退出。
		select {
		case <-leaderMigrator.MigrationReady:
			// 启动迁移锁竞争。
			wg.Go(func() {
				leaderElectAndRun(ctx, c, id, electionChecker,
					c.ComponentConfig.Generic.LeaderMigration.ResourceLock,
					c.ComponentConfig.Generic.LeaderMigration.LeaderName,
					leaderelection.LeaderCallbacks{
						OnStartedLeading: startedLeading(func(ctx context.Context) error {
							logger.Info("leader migration: starting migrated controllers.")
							controllerDescriptors := NewControllerDescriptors()
							controllerDescriptors = filteredControllerDescriptors(controllerDescriptors, leaderMigrator.FilterFunc, leadermigration.ControllerMigrated)
							// 不要在迁移锁下启动服务账号令牌控制器。
							delete(controllerDescriptors, names.ServiceAccountTokenController)
							return run(ctx, controllerDescriptors)
						}),
						OnStoppedLeading: func() {
							logger.Error(nil, "migration leaderelection lost/stopped")
							if !utilfeature.DefaultFeatureGate.Enabled(cmfeatures.ControllerManagerReleaseLeaderElectionLockOnExit) {
								klog.FlushAndExit(klog.ExitFlushTimeout, 1)
							}
						},
					})
			})

		case <-ctx.Done():
		}
	}

	// 等待全部领导者选举结束；此时协程均已终止，无需再加锁读取 errs。
	wg.Wait()
	return utilerrors.NewAggregate(errs)
}

// ControllerContext 汇总各控制器共享的客户端、informer 和配置。
type ControllerContext struct {
	// ClientBuilder 按控制器名称提供独立客户端。
	ClientBuilder clientbuilder.ControllerClientBuilder

	// InformerFactory 提供完整资源对象的共享 informer。
	InformerFactory informers.SharedInformerFactory

	// ObjectOrMetadataInformerFactory 提供静态类型对象及动态资源元数据 informer。
	// 目前通用控制器仅使用元数据；若未来需要完整对象，应改用带动态客户端的 GenericInformerFactory。
	ObjectOrMetadataInformerFactory informerfactory.InformerFactory

	// ComponentConfig 保存各控制器的组件配置。
	ComponentConfig kubectrlmgrconfig.KubeControllerManagerConfiguration

	// RESTMapper 在首次请求映射时才初始化发现结果。
	RESTMapper *restmapper.DeferredDiscoveryRESTMapper

	// 全部控制器初始化并运行后关闭 InformersStarted；此后控制器才可自行启动共享 informer。
	InformersStarted chan struct{}

	// ResyncPeriod 每次返回带随机抖动的时间，避免多个控制器同时冲击 API 服务器。
	ResyncPeriod func() time.Duration

	// ControllerManagerMetrics 提供控制器管理器专属指标。
	ControllerManagerMetrics *controllersmetrics.ControllerManagerMetrics

	// GraphBuilder 跟踪集群资源依赖关系。
	GraphBuilder *garbagecollector.GraphBuilder
}

// IsControllerEnabled 判断指定控制器是否启用。
func (c ControllerContext) IsControllerEnabled(controllerDescriptor *ControllerDescriptor) bool {
	controllersDisabledByDefault := sets.NewString()
	if controllerDescriptor.IsDisabledByDefault() {
		controllersDisabledByDefault.Insert(controllerDescriptor.Name())
	}
	return genericcontrollermanager.IsControllerEnabled(controllerDescriptor.Name(), controllersDisabledByDefault, c.ComponentConfig.Generic.Controllers)
}

// NewClientConfig 封装 ClientBuilder.Config，并为错误补充客户端名称。
func (c ControllerContext) NewClientConfig(name string) (*restclient.Config, error) {
	config, err := c.ClientBuilder.Config(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client config for %q: %w", name, err)
	}
	return config, nil
}

// NewClient 封装 ClientBuilder.Client，并为错误补充客户端名称。
func (c ControllerContext) NewClient(name string) (kubernetes.Interface, error) {
	client, err := c.ClientBuilder.Client(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client for %q: %w", name, err)
	}
	return client, nil
}

// CreateControllerContext 创建控制器共享上下文；rootClientBuilder 仅供共享 informer 和令牌控制器使用。
func CreateControllerContext(ctx context.Context, s *config.CompletedConfig, rootClientBuilder, clientBuilder clientbuilder.ControllerClientBuilder) (ControllerContext, error) {
	// informer 转换器移除 ManagedFields，以降低内存占用。
	trim := func(obj interface{}) (interface{}, error) {
		if accessor, err := meta.Accessor(obj); err == nil {
			if accessor.GetManagedFields() != nil {
				accessor.SetManagedFields(nil)
			}
		}
		return obj, nil
	}

	versionedClient, err := rootClientBuilder.Client("shared-informers")
	if err != nil {
		return ControllerContext{}, fmt.Errorf("failed to create Kubernetes client for %q: %w", "shared-informers", err)
	}

	informerName, err := cache.NewInformerName("kube-controller-manager")
	if err != nil {
		return ControllerContext{}, fmt.Errorf("failed to create informer name: %w", err)
	}

	sharedInformers := informers.NewSharedInformerFactoryWithOptions(versionedClient, ResyncPeriod(s)(), informers.WithTransform(trim), informers.WithInformerName(informerName))

	metadataConfig, err := rootClientBuilder.Config("metadata-informers")
	if err != nil {
		return ControllerContext{}, fmt.Errorf("failed to create metadata client config: %w", err)
	}

	metadataClient, err := metadata.NewForConfig(metadataConfig)
	if err != nil {
		return ControllerContext{}, fmt.Errorf("failed to create metadata client: %w", err)
	}

	metadataInformers := metadatainformer.NewSharedInformerFactoryWithOptions(metadataClient, ResyncPeriod(s)(), metadatainformer.WithTransform(trim))

	// API 服务器尚未就绪时先等待再失败，以支持 API 服务器与控制器管理器同时启动。
	if err := genericcontrollermanager.WaitForAPIServer(versionedClient, 10*time.Second); err != nil {
		return ControllerContext{}, fmt.Errorf("failed to wait for apiserver being healthy: %w", err)
	}

	// 使用支持刷新的发现客户端。
	discoveryClient, err := rootClientBuilder.DiscoveryClient("controller-discovery")
	if err != nil {
		return ControllerContext{}, fmt.Errorf("failed to create discovery client: %w", err)
	}

	cachedClient := cacheddiscovery.NewMemCacheClient(discoveryClient)
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedClient)
	go wait.Until(func() {
		restMapper.Reset()
	}, 30*time.Second, ctx.Done())

	controllerContext := ControllerContext{
		ClientBuilder:                   clientBuilder,
		InformerFactory:                 sharedInformers,
		ObjectOrMetadataInformerFactory: informerfactory.NewInformerFactory(sharedInformers, metadataInformers),
		ComponentConfig:                 s.ComponentConfig,
		RESTMapper:                      restMapper,
		InformersStarted:                make(chan struct{}),
		ResyncPeriod:                    ResyncPeriod(s),
		ControllerManagerMetrics:        controllersmetrics.NewControllerManagerMetrics(kubeControllerManager),
	}

	if controllerContext.ComponentConfig.GarbageCollectorController.EnableGarbageCollector &&
		controllerContext.IsControllerEnabled(NewControllerDescriptors()[names.GarbageCollectorController]) {
		ignoredResources := make(map[schema.GroupResource]struct{})
		for _, r := range controllerContext.ComponentConfig.GarbageCollectorController.GCIgnoredResources {
			ignoredResources[schema.GroupResource{Group: r.Group, Resource: r.Resource}] = struct{}{}
		}

		controllerContext.GraphBuilder = garbagecollector.NewDependencyGraphBuilder(
			ctx,
			metadataClient,
			controllerContext.RESTMapper,
			ignoredResources,
			controllerContext.ObjectOrMetadataInformerFactory,
			controllerContext.InformersStarted,
		)
	}

	controllersmetrics.Register()
	return controllerContext, nil
}

// HealthCheckAdder 抽象健康检查注册能力，便于测试替换具体处理器。
type HealthCheckAdder interface {
	AddHealthChecker(checks ...healthz.HealthChecker)
}

// BuildControllers 构造描述符映射中的已启用控制器，跳过禁用项。
// 每个控制器默认注册恒通过的健康检查；实现 controller.HealthCheckable 时使用其检查。
// 实现 controller.Debuggable 时，将调试处理器注册到给定 mux。
func BuildControllers(ctx context.Context, controllerCtx ControllerContext, controllerDescriptors map[string]*ControllerDescriptor,
	unsecuredMux *mux.PathRecorderMux, healthzHandler HealthCheckAdder) ([]Controller, error) {
	logger := klog.FromContext(ctx)
	var (
		controllers []Controller
		checks      []healthz.HealthChecker
	)
	buildController := func(controllerDesc *ControllerDescriptor) error {
		controllerName := controllerDesc.Name()
		ctrl, err := controllerDesc.BuildController(ctx, controllerCtx)
		if err != nil {
			logger.Error(err, "Error initializing a controller", "controller", controllerName)
			return err
		}
		if ctrl == nil {
			logger.Info("Warning: skipping controller", "controller", controllerName)
			return nil
		}

		check := controllerhealthz.NamedPingChecker(controllerName)
		// 仅在控制器提供调试处理器且 mux 可用时注册调试端点。
		if debuggable, ok := ctrl.(controller.Debuggable); ok && unsecuredMux != nil {
			if debugHandler := debuggable.DebuggingHandler(); debugHandler != nil {
				basePath := "/debug/controllers/" + controllerName
				unsecuredMux.UnlistedHandle(basePath, http.StripPrefix(basePath, debugHandler))
				unsecuredMux.UnlistedHandlePrefix(basePath+"/", http.StripPrefix(basePath, debugHandler))
			}
		}
		if healthCheckable, ok := ctrl.(controller.HealthCheckable); ok {
			if realCheck := healthCheckable.HealthChecker(); realCheck != nil {
				check = controllerhealthz.NamedHealthChecker(controllerName, realCheck)
			}
		}

		controllers = append(controllers, ctrl)
		checks = append(checks, check)
		return nil
	}

	// 服务账号令牌控制器必须用高权限客户端优先启动，为其余控制器签发令牌；失败则终止启动。
	if serviceAccountTokenControllerDescriptor, ok := controllerDescriptors[names.ServiceAccountTokenController]; ok {
		if err := buildController(serviceAccountTokenControllerDescriptor); err != nil {
			return nil, err
		}
	}

	// 各控制器使用带自身名称的日志上下文。
	// StartController 同时显式设置 "controller" 字段：上下文日志仍为 alpha 特性，
	// 且该字段可区分控制器自身与启动流程发出的日志。
	for _, controllerDesc := range controllerDescriptors {
		if controllerDesc.RequiresSpecialHandling() {
			continue
		}

		if !controllerCtx.IsControllerEnabled(controllerDesc) {
			logger.Info("Warning: controller is disabled", "controller", controllerDesc.Name())
			continue
		}

		if err := buildController(controllerDesc); err != nil {
			return nil, err
		}
	}

	// 注册全部健康检查。
	if len(checks) > 0 {
		healthzHandler.AddHealthChecker(checks...)
	}
	return controllers, nil
}

// RunControllers 并发运行全部控制器，并阻塞至上下文取消且所有控制器退出。
// 取消后最多等待 shutdownTimeout；超时返回 false，否则返回 true。零值表示不设超时。
func RunControllers(ctx context.Context, controllerCtx ControllerContext, controllers []Controller,
	controllerStartJitterMaxFactor float64, shutdownTimeout time.Duration) bool {
	logger := klog.FromContext(ctx)

	// 记录运行中的控制器名称，在关闭期间定期打印尚未退出的控制器。
	runningControllers := sets.New[string]()
	var runningControllersLock sync.Mutex

	loggingCtx, cancelLoggingCtx := context.WithCancel(context.Background())
	defer cancelLoggingCtx()
	go func() {
		// 仅在关闭阶段开始打印。
		select {
		case <-ctx.Done():
		case <-loggingCtx.Done():
			return
		}

		// 定期打印尚未退出的控制器。
		logPeriod := shutdownTimeout / 3
		if logPeriod == 0 {
			logPeriod = 5 * time.Second
		}
		ticker := time.NewTicker(logPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				runningControllersLock.Lock()
				running := sets.List(runningControllers)
				runningControllersLock.Unlock()

				logger.Info("Still waiting for some controllers to terminate...", "runningControllers", running)

			case <-loggingCtx.Done():
				return
			}
		}
	}()

	terminatedCh := make(chan struct{})
	go func() {
		defer close(terminatedCh)
		var wg sync.WaitGroup
		wg.Add(len(controllers))
		for _, controller := range controllers {
			go func() {
				defer wg.Done()

				// 此处不随上下文取消提前返回，否则会加剧测试时序波动。
				time.Sleep(wait.Jitter(controllerCtx.ComponentConfig.Generic.ControllerStartInterval.Duration, controllerStartJitterMaxFactor))

				logger.V(1).Info("Controller starting...", "controller", controller.Name())

				runningControllersLock.Lock()
				runningControllers.Insert(controller.Name())
				runningControllersLock.Unlock()

				defer func() {
					logger.V(1).Info("Controller terminated", "controller", controller.Name())

					runningControllersLock.Lock()
					runningControllers.Delete(controller.Name())
					runningControllersLock.Unlock()
				}()
				controller.Run(ctx)
			}()
		}
		wg.Wait()
		logger.Info("All controllers terminated")
	}()

	// 等待上下文取消或全部控制器自然结束。
	select {
	case <-ctx.Done():
	case <-terminatedCh:
		return true
	}

	// 在关闭超时范围内等待全部控制器退出。
	var shutdownCh <-chan time.Time
	if shutdownTimeout > 0 {
		shutdownCh = time.After(shutdownTimeout)
	}
	select {
	case <-terminatedCh:
		return true
	case <-shutdownCh:
		runningControllersLock.Lock()
		running := sets.List(runningControllers)
		runningControllersLock.Unlock()
		logger.Info("Controller shutdown timeout reached", "timeout", shutdownTimeout, "runningControllers", running)
		return false
	}
}

func readCA(file string) ([]byte, error) {
	rootCA, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	if _, err := certutil.ParseCertsPEM(rootCA); err != nil {
		return nil, err
	}

	return rootCA, err
}

// createClientBuilders 根据配置创建按控制器隔离的客户端构建器和根客户端构建器。
func createClientBuilders(c *config.CompletedConfig) (clientBuilder clientbuilder.ControllerClientBuilder, rootClientBuilder clientbuilder.ControllerClientBuilder) {

	rootClientBuilder = clientbuilder.SimpleControllerClientBuilder{
		ClientConfig: c.Kubeconfig,
	}
	if c.ComponentConfig.KubeCloudShared.UseServiceAccountCredentials {

		clientBuilder = clientbuilder.NewDynamicClientBuilder(
			restclient.AnonymousClientConfig(c.Kubeconfig),
			c.Client.CoreV1(),
			metav1.NamespaceSystem)
	} else {
		clientBuilder = rootClientBuilder
	}
	return
}

// leaderElectAndRun 运行领导者选举，取得租约后执行回调。
// TODO：将此函数提取到 staging/controller-manager。
func leaderElectAndRun(ctx context.Context, c *config.CompletedConfig, lockIdentity string, electionChecker *leaderelection.HealthzAdaptor, resourceLock string, leaseName string, callbacks leaderelection.LeaderCallbacks) {
	logger := klog.FromContext(ctx)
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
		logger.Error(err, "Error creating lock")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   c.ComponentConfig.Generic.LeaderElection.LeaseDuration.Duration,
		RenewDeadline:   c.ComponentConfig.Generic.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:     c.ComponentConfig.Generic.LeaderElection.RetryPeriod.Duration,
		Callbacks:       callbacks,
		WatchDog:        electionChecker,
		ReleaseOnCancel: utilfeature.DefaultFeatureGate.Enabled(cmfeatures.ControllerManagerReleaseLeaderElectionLockOnExit),
		Name:            leaseName,
		Coordinated:     utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CoordinatedLeaderElection),
	})
}

// filteredControllerDescriptors 返回筛选结果符合 expected 的控制器描述符。
func filteredControllerDescriptors(controllerDescriptors map[string]*ControllerDescriptor, filterFunc leadermigration.FilterFunc, expected leadermigration.FilterResult) map[string]*ControllerDescriptor {
	resultControllers := make(map[string]*ControllerDescriptor)
	for name, controllerDesc := range controllerDescriptors {
		if filterFunc(name) == expected {
			resultControllers[name] = controllerDesc
		}
	}
	return resultControllers
}
