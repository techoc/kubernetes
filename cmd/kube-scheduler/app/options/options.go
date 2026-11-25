/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/server/flagz"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	basecompatibility "k8s.io/component-base/compatibility"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/config/options"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	metricsfeatures "k8s.io/component-base/metrics/features"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/klog/v2"
	schedulerappconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	netutils "k8s.io/utils/net"
)

// Options 聚合 Scheduler 的默认值和用户输入；ApplyTo/Config 再将它转换为运行时 Config。
type Options struct {
	// ComponentConfig 保存当前 API 版本的默认配置，之后由用户配置或命令行覆盖。
	ComponentConfig *kubeschedulerconfig.KubeSchedulerConfiguration

	// SecureServing 配置 HTTPS 监听和证书，BindPort 为零且未提供 Listener 时不开放端点。
	SecureServing *apiserveroptions.SecureServingOptions
	// Authentication 委托 API Server 认证请求，并允许测试环境缺少集群内配置。
	Authentication *apiserveroptions.DelegatingAuthenticationOptions
	// Authorization 配置管理端点的委托授权规则。
	Authorization *apiserveroptions.DelegatingAuthorizationOptions
	// Metrics 控制指标暴露与稳定性策略。
	Metrics *metrics.Options
	// Logs 保存日志格式、级别和输出选项。
	Logs *logs.Options
	// Deprecated 保存仅用于兼容旧部署的命令行参数。
	Deprecated *DeprecatedOptions
	// LeaderElection 提供选主参数默认值，实际生效值由组件配置和显式 Flag 共同决定。
	LeaderElection *componentbaseconfig.LeaderElectionConfiguration

	// ConfigFile 保存 Scheduler 组件配置文件路径。
	ConfigFile string

	// WriteConfigTo 保存默认配置的导出路径；指定后写出配置并退出。
	WriteConfigTo string

	// Master 覆盖 kubeconfig 中的 API Server 地址，仅用于兼容旧命令行参数。
	Master string

	// ComponentGlobalsRegistry 保存各组件有效版本与 Feature Gate。
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry

	// InformerName 标识 Scheduler 共享 Informer，用于 client-go Informer 指标。
	InformerName *cache.InformerName

	// Flags 保存已解析的命令行参数。
	Flags *cliflag.NamedFlagSets
}

// NewOptions 创建使用全局兼容性注册表的默认 Scheduler 选项，并确保默认组件已登记。
func NewOptions() *Options {
	// 正常进程共享默认注册表；它集中管理组件有效版本与 Feature Gate。
	componentGlobalsRegistry := compatibility.DefaultComponentGlobalsRegistry
	// 确保默认 Kubernetes 组件已登记，避免配置解析时缺少版本和特性开关上下文。
	if componentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent) == nil {
		featureGate := utilfeature.DefaultMutableFeatureGate
		effectiveVersion := compatibility.DefaultBuildEffectiveVersion()
		utilruntime.Must(componentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate))
	}
	return NewOptionsWithComponentGlobalsRegistry(componentGlobalsRegistry)
}

// NewOptionsWithComponentGlobalsRegistry 允许注入独立注册表；测试可借此隔离全局 Feature Gate 状态。
func NewOptionsWithComponentGlobalsRegistry(componentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry) *Options {
	o := &Options{
		SecureServing:  apiserveroptions.NewSecureServingOptions(),
		Authentication: apiserveroptions.NewDelegatingAuthenticationOptions(),
		Authorization:  apiserveroptions.NewDelegatingAuthorizationOptions(),
		Deprecated: &DeprecatedOptions{
			PodMaxInUnschedulablePodsDuration: 5 * time.Minute,
		},
		LeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
			LeaderElect:       true,
			LeaseDuration:     metav1.Duration{Duration: 15 * time.Second},
			RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
			RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
			ResourceLock:      "leases",
			ResourceName:      "kube-scheduler",
			ResourceNamespace: "kube-system",
		},
		Metrics:                  metrics.NewOptions(),
		Logs:                     logs.NewOptions(),
		ComponentGlobalsRegistry: componentGlobalsRegistry,
	}

	o.Authentication.TolerateInClusterLookupFailure = true
	o.Authentication.RemoteKubeConfigFileOptional = true
	o.Authorization.RemoteKubeConfigFileOptional = true

	// 默认留空证书目录，使 SecureServing 按需生成内存自签名证书，避免在磁盘上留下默认证书。
	o.SecureServing.ServerCert.CertDirectory = ""
	o.SecureServing.ServerCert.PairName = "kube-scheduler"
	o.SecureServing.BindPort = kubeschedulerconfig.DefaultKubeSchedulerPort

	o.initFlags()

	return o
}

// ApplyDeprecated 只迁移用户显式指定的旧命令行参数，避免旧参数默认值覆盖新配置。
func (o *Options) ApplyDeprecated() {
	if o.Flags == nil {
		return
	}
	// 仅迁移命令行中显式指定的废弃参数。
	deprecated := o.Flags.FlagSet("deprecated")
	if deprecated.Changed("profiling") {
		o.ComponentConfig.EnableProfiling = o.Deprecated.EnableProfiling
	}
	if deprecated.Changed("contention-profiling") {
		o.ComponentConfig.EnableContentionProfiling = o.Deprecated.EnableContentionProfiling
	}
	if deprecated.Changed("kubeconfig") {
		o.ComponentConfig.ClientConnection.Kubeconfig = o.Deprecated.Kubeconfig
	}
	if deprecated.Changed("kube-api-content-type") {
		o.ComponentConfig.ClientConnection.ContentType = o.Deprecated.ContentType
	}
	if deprecated.Changed("kube-api-qps") {
		o.ComponentConfig.ClientConnection.QPS = o.Deprecated.QPS
	}
	if deprecated.Changed("kube-api-burst") {
		o.ComponentConfig.ClientConnection.Burst = o.Deprecated.Burst
	}
}

// ApplyLeaderElectionTo 将用户显式指定的选主参数覆盖到目标配置，并保存该配置供后续使用。
func (o *Options) ApplyLeaderElectionTo(cfg *kubeschedulerconfig.KubeSchedulerConfiguration) {
	if o.Flags == nil {
		return
	}
	// 仅覆盖命令行中显式指定的选主参数。
	leaderelection := o.Flags.FlagSet("leader election")
	if leaderelection.Changed("leader-elect") {
		cfg.LeaderElection.LeaderElect = o.LeaderElection.LeaderElect
	}
	if leaderelection.Changed("leader-elect-lease-duration") {
		cfg.LeaderElection.LeaseDuration = o.LeaderElection.LeaseDuration
	}
	if leaderelection.Changed("leader-elect-renew-deadline") {
		cfg.LeaderElection.RenewDeadline = o.LeaderElection.RenewDeadline
	}
	if leaderelection.Changed("leader-elect-retry-period") {
		cfg.LeaderElection.RetryPeriod = o.LeaderElection.RetryPeriod
	}
	if leaderelection.Changed("leader-elect-resource-lock") {
		cfg.LeaderElection.ResourceLock = o.LeaderElection.ResourceLock
	}
	if leaderelection.Changed("leader-elect-resource-name") {
		cfg.LeaderElection.ResourceName = o.LeaderElection.ResourceName
	}
	if leaderelection.Changed("leader-elect-resource-namespace") {
		cfg.LeaderElection.ResourceNamespace = o.LeaderElection.ResourceNamespace
	}

	o.ComponentConfig = cfg
}

// initFlags 按用途分组注册命令行参数；重复调用时直接返回，避免同名 Flag 冲突。
func (o *Options) initFlags() {
	if o.Flags != nil {
		return
	}

	nfs := cliflag.NamedFlagSets{}
	fs := nfs.FlagSet("misc")
	fs.StringVar(&o.ConfigFile, "config", o.ConfigFile, "The path to the configuration file.")
	fs.StringVar(&o.WriteConfigTo, "write-config-to", o.WriteConfigTo, "If set, write the configuration values to this file and exit.")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")

	o.SecureServing.AddFlags(nfs.FlagSet("secure serving"))
	o.Authentication.AddFlags(nfs.FlagSet("authentication"))
	o.Authorization.AddFlags(nfs.FlagSet("authorization"))
	o.Deprecated.AddFlags(nfs.FlagSet("deprecated"))
	options.BindLeaderElectionFlags(o.LeaderElection, nfs.FlagSet("leader election"))
	o.ComponentGlobalsRegistry.AddFlags(nfs.FlagSet("feature gate"))
	o.Metrics.AddFlags(nfs.FlagSet("metrics"))
	logsapi.AddFlags(o.Logs, nfs.FlagSet("logs"))

	o.Flags = &nfs
}

// ApplyTo 按配置优先级将 Options 应用到运行时 Config，并初始化服务端认证、授权和指标设置。
func (o *Options) ApplyTo(logger klog.Logger, c *schedulerappconfig.Config) error {
	if err := o.ComponentGlobalsRegistry.SetFallback(); err != nil {
		return err
	}
	// 未提供 --config 时以默认组件配置为基础，再迁移用户显式指定的旧参数和选主参数。
	if len(o.ConfigFile) == 0 {
		o.ApplyDeprecated()
		o.ApplyLeaderElectionTo(o.ComponentConfig)
		c.ComponentConfig = *o.ComponentConfig
	} else {
		// 提供 --config 时，文件是主要配置来源；为兼容性仅允许 Leader Election 命令行参数继续覆盖。
		cfg, err := LoadConfigFromFile(logger, o.ConfigFile)
		if err != nil {
			return err
		}
		o.ApplyLeaderElectionTo(cfg)

		if err := validation.ValidateKubeSchedulerConfiguration(cfg); err != nil {
			return err
		}

		c.ComponentConfig = *cfg
	}

	// 先构建 kubeconfig，再初始化 SecureServing，避免后续 kubeconfig 失败时遗留服务初始化阶段启动的 goroutine。
	kubeConfig, err := createKubeConfig(c.ComponentConfig.ClientConnection, o.Master)
	if err != nil {
		return err
	}
	c.KubeConfig = kubeConfig

	if err := o.SecureServing.ApplyTo(&c.SecureServing); err != nil {
		return err
	}
	if o.SecureServing != nil && (o.SecureServing.BindPort != 0 || o.SecureServing.Listener != nil) {
		if err := o.Authentication.ApplyTo(&c.Authentication, c.SecureServing, nil); err != nil {
			return err
		}
		if err := o.Authorization.ApplyTo(&c.Authorization); err != nil {
			return err
		}
	}
	metricsfeatures.ApplyFeatureGates(utilfeature.DefaultFeatureGate)
	o.Metrics.Apply()

	// 该值不能通过 ComponentConfig 设置，因此独立迁移，而不复用 ApplyDeprecated。
	if o.Deprecated != nil {
		c.PodMaxInUnschedulablePodsDuration = o.Deprecated.PodMaxInUnschedulablePodsDuration
	}
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		if o.Flags != nil {
			c.Flagz = flagz.NamedFlagSetsReader{FlagSets: *o.Flags}
		}
	}

	return nil
}

// Validate 汇总各子系统的校验错误，方便用户一次修正多项配置。
func (o *Options) Validate() []error {
	var errs []error
	if err := o.ComponentGlobalsRegistry.SetFallback(); err != nil {
		errs = append(errs, err)
	} else {
		errs = append(errs, o.ComponentGlobalsRegistry.Validate()...)
	}
	if err := validation.ValidateKubeSchedulerConfiguration(o.ComponentConfig); err != nil {
		errs = append(errs, err.Errors()...)
	}
	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)
	errs = append(errs, o.Authorization.Validate()...)
	errs = append(errs, o.Metrics.Validate()...)

	return errs
}

// Config 将 Options 转换为运行时 Config，并创建 API 客户端、事件广播器和 Informer。
func (o *Options) Config(ctx context.Context) (*schedulerappconfig.Config, error) {
	logger := klog.FromContext(ctx)
	if o.SecureServing != nil {
		if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
			return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
		}
	}

	c := &schedulerappconfig.Config{}
	if err := o.ApplyTo(logger, c); err != nil {
		return nil, err
	}

	// 普通资源访问和事件写入使用独立 Client；事件广播器只持有专用的 eventClient。
	client, eventClient, err := createClients(c.KubeConfig)
	if err != nil {
		return nil, err
	}

	c.EventBroadcaster = events.NewEventBroadcasterAdapterWithContext(ctx, eventClient)

	// 只有开启 LeaderElect 才创建 Lease 锁配置；关闭选主时保持 nil，Run 会走单实例分支。
	var leaderElectionConfig *leaderelection.LeaderElectionConfig
	if c.ComponentConfig.LeaderElection.LeaderElect {
		schedulerName := corev1.DefaultSchedulerName
		if len(c.ComponentConfig.Profiles) != 0 {
			schedulerName = c.ComponentConfig.Profiles[0].SchedulerName
		}
		// 选主事件使用首个 Profile 的 SchedulerName，使事件来源与该调度器实例配置保持一致。
		coreRecorder := c.EventBroadcaster.DeprecatedNewLegacyRecorder(schedulerName)
		leaderElectionConfig, err = makeLeaderElectionConfig(c.ComponentConfig.LeaderElection, c.KubeConfig, coreRecorder)
		if err != nil {
			return nil, err
		}
	}

	// 内置资源工厂提供静态类型的共享缓存；动态工厂用于访问编译期未知的资源类型。
	c.Client = client
	c.InformerFactory = scheduler.NewInformerFactory(client, 0, o.InformerName)
	dynClient := dynamic.NewForConfigOrDie(c.KubeConfig)
	c.DynInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, corev1.NamespaceAll, nil)
	c.LeaderElection = leaderElectionConfig
	c.ComponentGlobalsRegistry = o.ComponentGlobalsRegistry

	return c, nil
}

// makeLeaderElectionConfig 创建与给定配置关联的资源锁，并构造选主参数。
func makeLeaderElectionConfig(config componentbaseconfig.LeaderElectionConfiguration, kubeConfig *restclient.Config, recorder record.EventRecorder) (*leaderelection.LeaderElectionConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("unable to get hostname: %v", err)
	}
	// hostname 便于定位实例，UUID 避免同一台机器上多个 Scheduler 进程使用相同锁身份。
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.NewFromKubeconfig(config.ResourceLock,
		config.ResourceNamespace,
		config.ResourceName,
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
		kubeConfig,
		config.RenewDeadline.Duration)
	if err != nil {
		return nil, fmt.Errorf("couldn't create resource lock: %v", err)
	}

	// LeaseDuration、RenewDeadline、RetryPeriod 控制租约有效期、续约容忍窗口和竞争重试频率。
	// ReleaseOnCancel 允许正常退出时主动释放锁，减少下一个实例接管的等待时间。
	return &leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   config.LeaseDuration.Duration,
		RenewDeadline:   config.RenewDeadline.Duration,
		RetryPeriod:     config.RetryPeriod.Duration,
		WatchDog:        leaderelection.NewLeaderHealthzAdaptor(time.Second * 20),
		Name:            "kube-scheduler",
		ReleaseOnCancel: true,
	}, nil
}

// createKubeConfig 根据客户端连接配置和旧版 master 覆盖值创建 REST 配置。
// TODO: 移除旧命令行参数后删除 masterOverride。
func createKubeConfig(config componentbaseconfig.ClientConnectionConfiguration, masterOverride string) (*restclient.Config, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(masterOverride, config.Kubeconfig)
	if err != nil {
		return nil, err
	}

	// 沿用组件配置中的序列化类型、QPS 和 Burst；Scheduler 客户端按既定行为禁用 HTTP 压缩。
	kubeConfig.DisableCompression = true
	kubeConfig.AcceptContentTypes = config.AcceptContentTypes
	kubeConfig.ContentType = config.ContentType
	kubeConfig.QPS = config.QPS
	kubeConfig.Burst = int(config.Burst)

	return kubeConfig, nil
}

// createClients 分离普通资源访问和事件写入客户端，避免事件流与主 API 请求混为一组配置。
func createClients(kubeConfig *restclient.Config) (clientset.Interface, clientset.Interface, error) {
	// 业务 Client 添加 scheduler User-Agent，便于 API Server 识别请求来源。
	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "scheduler"))
	if err != nil {
		return nil, nil, err
	}

	// Event Client 单独创建，专门交给 EventBroadcaster 写事件。
	eventClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, nil, err
	}

	return client, eventClient, nil
}
