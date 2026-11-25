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

// Package app 负责 kube-proxy 的配置、启动与运行。
package app

import (
	"context"
	goflag "flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/server/statusz"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/events"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	metricsfeatures "k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	configv1alpha1 "k8s.io/kube-proxy/config/v1alpha1"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/kubernetes/pkg/proxy/apis"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	proxyconfigscheme "k8s.io/kubernetes/pkg/proxy/apis/config/scheme"
	v1alpha1conversion "k8s.io/kubernetes/pkg/proxy/apis/config/v1alpha1"
	"k8s.io/kubernetes/pkg/proxy/config"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	proxymetrics "k8s.io/kubernetes/pkg/proxy/metrics"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	"k8s.io/kubernetes/pkg/util/oom"
	netutils "k8s.io/utils/net"
)

const (
	// kubeProxy 是内部引用 kube-proxy 组件时使用的名称。
	kubeProxy = "kube-proxy"
)

func init() {
	utilruntime.Must(metricsfeatures.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// proxyRun 抽象代理服务器的运行入口，便于测试替换实现。
type proxyRun interface {
	// Run 启动代理并阻塞至其结束；返回 nil 表示正常结束。
	Run(ctx context.Context) error
}

// NewProxyCommand 创建使用默认配置的 kube-proxy cobra 命令。
// 命令依次完成平台初始化、配置汇总与校验，再运行代理服务器。
func NewProxyCommand() *cobra.Command {
	opts := NewOptions()

	cmd := &cobra.Command{
		Use: kubeProxy,
		Long: `The Kubernetes network proxy runs on each node. This
reflects services as defined in the Kubernetes API on each node and can do simple
TCP, UDP, and SCTP stream forwarding or round robin TCP, UDP, and SCTP forwarding across a set of backends.
Service cluster IPs and ports are currently found through Docker-links-compatible
environment variables specifying ports opened by the service proxy. There is an optional
addon that provides cluster DNS for these cluster IPs. The user must create a service
with the apiserver API to configure the proxy.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()

			if err := initForOS(opts.config.Windows.RunAsService); err != nil {
				return fmt.Errorf("failed os init: %w", err)
			}

			if err := opts.Complete(cmd.Flags()); err != nil {
				return fmt.Errorf("failed complete: %w", err)
			}

			logs.InitLogs()
			if err := logsapi.ValidateAndApplyAsField(&opts.config.Logging, utilfeature.DefaultFeatureGate, field.NewPath("logging")); err != nil {
				return fmt.Errorf("initialize logging: %w", err)
			}

			cliflag.PrintFlags(cmd.Flags())

			if err := opts.Validate(); err != nil {
				return fmt.Errorf("failed validate: %w", err)
			}
			// 注册功能开关状态指标，便于确认实际生效的配置。
			utilfeature.DefaultMutableFeatureGate.AddMetrics()
			if err := opts.Run(context.Background()); err != nil {
				opts.logger.Error(err, "Error running ProxyServer")
				return err
			}

			return nil
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
	opts.AddFlags(fs)
	fs.AddGoFlagSet(goflag.CommandLine) // 加入 --boot-id-file 与 --machine-id-file

	_ = cmd.MarkFlagFilename("config", "yaml", "yml", "json")

	return cmd
}

// ProxyServer 保存启动 Kubernetes 网络代理所需的全部运行时依赖与状态。
type ProxyServer struct {
	// Config 是已完成默认值转换的 kube-proxy 内部配置。
	Config *kubeproxyconfig.KubeProxyConfiguration

	// Client 用于监视 API 对象并发送事件。
	Client clientset.Interface
	// Broadcaster 将事件异步投递到 API 服务器。
	Broadcaster events.EventBroadcaster
	// Recorder 记录代理启动和健康检查等事件。
	Recorder events.EventRecorder
	// NodeRef 是事件关联的当前节点引用。
	NodeRef *v1.ObjectReference
	// HealthzServer 是代理健康检查服务；配置为空监听地址时为 nil。
	HealthzServer *healthcheck.ProxyHealthServer
	// NodeName 是当前 kube-proxy 所属节点名称。
	NodeName string
	// PrimaryIPFamily 是节点优先使用的 IP 协议族。
	PrimaryIPFamily v1.IPFamily
	// NodeIPs 保存各协议族对应的节点 IP，缺失族可能使用回环地址兜底。
	NodeIPs map[v1.IPFamily]net.IP
	flagz   flagz.Reader

	// podCIDRs 仅在 LocalModeNodeCIDR 模式下用于判断本地流量。
	podCIDRs []string
	// NodeManager 监视当前节点；hollow-proxy 不需要节点配置时为 nil。
	NodeManager *proxy.NodeManager

	// Proxier 按所选模式实现服务流量转发与规则同步。
	Proxier proxy.Provider
}

// newProxyServer 根据给定配置创建 ProxyServer，完成客户端、节点信息、平台检查和 Proxier 初始化。
// initOnly 为 true 时仍执行初始化，但由 Options.Run 决定是否继续运行。
func newProxyServer(ctx context.Context, config *kubeproxyconfig.KubeProxyConfiguration, master string, initOnly bool, flagzReader flagz.Reader) (*ProxyServer, error) {
	logger := klog.FromContext(ctx)

	s := &ProxyServer{
		Config: config,
		flagz:  flagzReader,
	}

	externalConfig := &configv1alpha1.KubeProxyConfiguration{}
	if err := v1alpha1conversion.Convert_config_KubeProxyConfiguration_To_v1alpha1_KubeProxyConfiguration(config, externalConfig, nil); err != nil {
		return nil, fmt.Errorf("unable to convert configz: %w", err)
	}
	externalConfig.SetGroupVersionKind(configv1alpha1.SchemeGroupVersion.WithKind("KubeProxyConfiguration"))

	cz, err := configz.New(kubeproxyconfig.GroupName)
	if err != nil {
		return nil, fmt.Errorf("unable to register configz: %w", err)
	}
	if err := cz.Set(externalConfig); err != nil {
		return nil, fmt.Errorf("unable to set configz: %w", err)
	}

	if len(config.ShowHiddenMetricsForVersion) > 0 {
		metrics.SetShowHidden()
	}

	s.NodeName, err = nodeutil.GetHostname(config.HostnameOverride)
	if err != nil {
		return nil, err
	}

	s.Client, err = createClient(ctx, config.ClientConnection, master)
	if err != nil {
		return nil, err
	}

	// NodeManager 通过 informer 只监视 kube-proxy 所在节点。
	s.NodeManager, err = proxy.NewNodeManager(ctx, s.Client, s.NodeName, s.Config)
	if err != nil {
		return nil, err
	}

	rawNodeIPs := s.NodeManager.NodeIPs()
	if len(rawNodeIPs) > 0 {
		logger.Info("Successfully retrieved NodeIPs", "NodeIPs", rawNodeIPs)
	}
	s.PrimaryIPFamily, s.NodeIPs = detectNodeIPs(ctx, rawNodeIPs, config.BindAddress)
	s.podCIDRs = s.NodeManager.PodCIDRs()

	config.NodePortAddresses = expandNodePortAddressKeywords(config.NodePortAddresses, s.NodeIPs)

	s.Broadcaster = events.NewBroadcaster(&events.EventSinkImpl{Interface: s.Client.EventsV1()})
	s.Recorder = s.Broadcaster.NewRecorder(proxyconfigscheme.Scheme, kubeProxy)

	s.NodeRef = &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       s.NodeName,
		Namespace:  "",
	}

	if len(config.HealthzBindAddress) > 0 {
		s.HealthzServer = healthcheck.NewProxyHealthServer(config.HealthzBindAddress, 2*config.SyncPeriod.Duration, s.NodeManager)
	}

	err = s.platformSetup(ctx)
	if err != nil {
		return nil, err
	}

	err = checkBadConfig(s)
	if err != nil {
		logger.Error(err, "Kube-proxy configuration may be incomplete or incorrect")
	}

	ipv4Supported, ipv6Supported, dualStackSupported, err := s.platformCheckSupported(ctx)
	if err != nil {
		return nil, err
	} else if (s.PrimaryIPFamily == v1.IPv4Protocol && !ipv4Supported) || (s.PrimaryIPFamily == v1.IPv6Protocol && !ipv6Supported) {
		return nil, fmt.Errorf("no support for primary IP family %q", s.PrimaryIPFamily)
	} else if dualStackSupported {
		logger.Info("kube-proxy running in dual-stack mode", "primary ipFamily", s.PrimaryIPFamily)
	} else {
		logger.Info("kube-proxy running in single-stack mode", "ipFamily", s.PrimaryIPFamily)
	}

	err, fatal := checkBadIPConfig(s, dualStackSupported)
	if err != nil {
		if fatal {
			return nil, fmt.Errorf("kube-proxy configuration is incorrect: %w", err)
		}
		logger.Error(err, "Kube-proxy configuration may be incomplete or incorrect")
	}

	s.Proxier, err = s.createProxier(ctx, config, dualStackSupported, initOnly)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// checkBadConfig 检查可能扩大 NodePort 暴露面的配置，并返回兼容性警告。
func checkBadConfig(s *ProxyServer) error {
	var errors []error

	// 此时尚未观察到实际 Service 或 EndpointSlice，只能根据显式配置判断是否存在双栈意图。
	anyDualStackConfig := false
	for _, config := range [][]string{s.Config.DetectLocal.ClusterCIDRs, s.Config.NodePortAddresses, s.Config.IPVS.ExcludeCIDRs, s.podCIDRs} {
		if dual, _ := netutils.IsDualStackCIDRStrings(config); dual {
			anyDualStackConfig = true
			break
		}
	}

	// NodePortAddresses 未覆盖所有在用协议族时，NodePort 可能暴露在非预期本地 IP 上。
	cidrsByFamily := proxyutil.MapCIDRsByIPFamily(s.Config.NodePortAddresses)
	if len(s.Config.NodePortAddresses) == 0 {
		errors = append(errors, fmt.Errorf("nodePortAddresses is unset; NodePort connections will be accepted on all local IPs. Consider using `--nodeport-addresses primary`"))
	} else if anyDualStackConfig && len(cidrsByFamily[s.PrimaryIPFamily]) == len(s.Config.NodePortAddresses) {
		errors = append(errors, fmt.Errorf("cluster appears to be dual-stack but nodePortAddresses contains only %s addresses; NodePort connections will be accepted on all local %s IPs", s.PrimaryIPFamily, proxyutil.OtherIPFamily(s.PrimaryIPFamily)))
	} else if len(cidrsByFamily[s.PrimaryIPFamily]) == 0 {
		errors = append(errors, fmt.Errorf("cluster appears to be %s-primary but nodePortAddresses contains only %s addresses; NodePort connections will be accepted on all local %s IPs", s.PrimaryIPFamily, proxyutil.OtherIPFamily(s.PrimaryIPFamily), s.PrimaryIPFamily))
	}

	return utilerrors.NewAggregate(errors)
}

// checkBadIPConfig 检查配置与 PrimaryIPFamily 是否冲突，并区分警告与致命错误。
// 历史选项的协议族不匹配不能突然变为致命错误；新增检查项应默认按致命错误处理。
func checkBadIPConfig(s *ProxyServer, dualStackSupported bool) (err error, fatal bool) {
	var errors []error
	var badFamily netutils.IPFamily

	if s.PrimaryIPFamily == v1.IPv4Protocol {
		badFamily = netutils.IPv6
	} else {
		badFamily = netutils.IPv4
	}

	var clusterType string
	if dualStackSupported {
		clusterType = fmt.Sprintf("%s-primary", s.PrimaryIPFamily)
	} else {
		clusterType = fmt.Sprintf("%s-only", s.PrimaryIPFamily)
	}

	if badCIDRs(s.Config.DetectLocal.ClusterCIDRs, badFamily) {
		errors = append(errors, fmt.Errorf("cluster is %s but clusterCIDRs contains only IPv%s addresses", clusterType, badFamily))
		if s.Config.DetectLocalMode == kubeproxyconfig.LocalModeClusterCIDR && !dualStackSupported {
			// 该组合历史上就是致命错误，保持兼容行为。
			fatal = true
		}
	}

	if badCIDRs(s.podCIDRs, badFamily) {
		errors = append(errors, fmt.Errorf("cluster is %s but node.spec.podCIDRs contains only IPv%s addresses", clusterType, badFamily))
		if s.Config.DetectLocalMode == kubeproxyconfig.LocalModeNodeCIDR {
			// 该组合历史上就是致命错误，保持兼容行为。
			fatal = true
		}
	}

	if netutils.IPFamilyOfString(s.Config.Winkernel.SourceVip) == badFamily {
		errors = append(errors, fmt.Errorf("cluster is %s but winkernel.sourceVip is IPv%s", clusterType, badFamily))
	}

	// 次要协议族完全不存在时，错误协议族配置才会造成额外问题。
	if !dualStackSupported {
		if badCIDRs(s.Config.IPVS.ExcludeCIDRs, badFamily) {
			errors = append(errors, fmt.Errorf("cluster is %s but ipvs.excludeCIDRs contains only IPv%s addresses", clusterType, badFamily))
		}

		if badBindAddress(s.Config.HealthzBindAddress, badFamily) {
			errors = append(errors, fmt.Errorf("cluster is %s but healthzBindAddress is IPv%s", clusterType, badFamily))
		}
		if badBindAddress(s.Config.MetricsBindAddress, badFamily) {
			errors = append(errors, fmt.Errorf("cluster is %s but metricsBindAddress is IPv%s", clusterType, badFamily))
		}
	}

	// NodePortAddresses 已在 checkBadConfig 中检查，无需重复。

	return utilerrors.NewAggregate(errors), fatal
}

// badCIDRs 判断 cidrs 是否非空且所有条目都属于错误协议族。
// 空列表或混合协议族均返回 false，因为此时不能确定是单协议族配置错误。
func badCIDRs(cidrs []string, wrongFamily netutils.IPFamily) bool {
	if len(cidrs) == 0 {
		return false
	}
	for _, cidr := range cidrs {
		if netutils.IPFamilyOfCIDRString(cidr) != wrongFamily {
			return false
		}
	}
	return true
}

// badBindAddress 判断 "IP:port" 是否绑定到错误协议族的非通配地址。
// 通配地址同时适用于双栈监听，因此不算冲突。
func badBindAddress(bindAddress string, wrongFamily netutils.IPFamily) bool {
	if host, _, _ := net.SplitHostPort(bindAddress); host != "" {
		ip := netutils.ParseIPSloppy(host)
		if ip != nil && netutils.IPFamilyOf(ip) == wrongFamily && !ip.IsUnspecified() {
			return true
		}
	}
	return false
}

// createClient 根据配置和 masterOverride 创建 Kubernetes 客户端。
// 未指定 kubeconfig 与 master 时使用集群内配置。
// TODO: 删除对应命令行标志时移除 masterOverride。
func createClient(ctx context.Context, config componentbaseconfig.ClientConnectionConfiguration, masterOverride string) (clientset.Interface, error) {
	logger := klog.FromContext(ctx)
	var kubeConfig *rest.Config
	var err error

	if len(config.Kubeconfig) == 0 && len(masterOverride) == 0 {
		logger.Info("Neither kubeconfig file nor master URL was specified, falling back to in-cluster config")
		kubeConfig, err = rest.InClusterConfig()
	} else {
		// 先加载指定的 kubeconfig，再用非空的 master 覆盖 API 服务器地址。
		kubeConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: config.Kubeconfig},
			&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterOverride}}).ClientConfig()
	}
	if err != nil {
		return nil, err
	}

	kubeConfig.AcceptContentTypes = config.AcceptContentTypes
	kubeConfig.ContentType = config.ContentType
	kubeConfig.QPS = config.QPS
	kubeConfig.Burst = int(config.Burst)

	client, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func serveHealthz(ctx context.Context, hz *healthcheck.ProxyHealthServer, errCh chan error) {
	logger := klog.FromContext(ctx)
	if hz == nil {
		return
	}

	fn := func(ctx context.Context) {
		err := hz.Run(ctx)
		if err != nil {
			logger.Error(err, "Healthz server failed")
			if errCh != nil {
				errCh <- fmt.Errorf("healthz server failed: %w", err)
				// 硬失败模式下不重试，永久阻塞以把错误传递给主循环。
				blockCh := make(chan error)
				<-blockCh
			}
		} else {
			logger.Error(nil, "Healthz server returned without error")
		}
	}
	go wait.UntilWithContext(ctx, fn, 5*time.Second)
}

func serveMetrics(ctx context.Context, bindAddress string, proxyMode kubeproxyconfig.ProxyMode, enableProfiling bool, flagzReader flagz.Reader, errCh chan error) {
	if len(bindAddress) == 0 {
		return
	}

	proxyMux := mux.NewPathRecorderMux(kubeProxy)
	healthz.InstallHandler(proxyMux)
	slis.SLIMetricsWithReset{}.Install(proxyMux)

	proxyMux.HandleFunc("/proxyMode", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		fmt.Fprintf(w, "%s", proxyMode)
	})

	proxyMux.Handle("/metrics", legacyregistry.Handler())

	if enableProfiling {
		routes.Profiling{}.Install(proxyMux)
		routes.DebugFlags{}.Install(proxyMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}

	configz.InstallHandler(proxyMux)

	if flagzReader != nil {
		flagz.Install(proxyMux, "kube-proxy", flagzReader)
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		reg := statusz.NewRegistry(
			compatibility.DefaultBuildEffectiveVersion(),
			statusz.WithListedPaths(proxyMux.ListedPaths()),
		)
		statusz.Install(proxyMux, kubeProxy, reg)
	}

	fn := func(ctx context.Context) {
		var err error
		defer func() {
			if err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "starting metrics server failed")
				if errCh != nil {
					errCh <- err
					// 硬失败模式下不重试，永久阻塞以把错误传递给主循环。
					blockCh := make(chan error)
					<-blockCh
				}
			}
		}()

		listener, err := netutils.MultiListen(ctx, "tcp", bindAddress)
		if err != nil {
			return
		}

		server := &http.Server{Handler: proxyMux}
		err = server.Serve(listener)
		if err != nil {
			return
		}

	}
	go wait.UntilWithContext(ctx, fn, 5*time.Second)
}

// Run 启动代理及辅助服务，并等待辅助服务报告错误。
// 当前正常路径不会自行返回；若返回 nil，调用方可能永不退出，需能处理 nil 错误。
func (s *ProxyServer) Run(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	// 启动时立即记录版本，便于在故障发生前确认实际二进制版本。
	logger.Info("Version info", "version", version.Get())

	logger.Info("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	proxymetrics.RegisterMetrics(s.Config.Mode)

	// TODO(vmarmol): 改为从容器配置读取。
	var oomAdjuster *oom.OOMAdjuster
	if s.Config.Linux.OOMScoreAdj != nil {
		oomAdjuster = oom.NewOOMAdjuster()
		if err := oomAdjuster.ApplyOOMScoreAdj(0, int(*s.Config.Linux.OOMScoreAdj)); err != nil {
			logger.V(2).Info("Failed to apply OOMScore", "err", err)
		}
	}

	if s.Broadcaster != nil {
		stopCh := make(chan struct{})
		s.Broadcaster.StartRecordingToSink(stopCh)
	}

	// TODO(thockin): 支持健康检查与指标共用同一端口。

	var healthzErrCh, metricsErrCh chan error
	if s.Config.BindAddressHardFail {
		healthzErrCh = make(chan error)
		metricsErrCh = make(chan error)
	}

	// 仅在配置了监听地址时启动健康检查服务。
	serveHealthz(ctx, s.HealthzServer, healthzErrCh)

	// 仅在配置了监听地址时启动指标服务。
	serveMetrics(ctx, s.Config.MetricsBindAddress, s.Config.Mode, s.Config.EnableProfiling, s.flagz, metricsErrCh)

	noProxyName, err := labels.NewRequirement(apis.LabelServiceProxyName, selection.DoesNotExist, nil)
	if err != nil {
		return err
	}

	noHeadlessEndpoints, err := labels.NewRequirement(v1.IsHeadlessService, selection.DoesNotExist, nil)
	if err != nil {
		return err
	}

	labelSelectorNoProxyName := labels.NewSelector().Add(*noProxyName)
	labelSelectorNoHeadlessEndpoints := labels.NewSelector().Add(*noHeadlessEndpoints)

	// kube-proxy 不使用 managedFields，缓存前统一剥离以减少内存占用。
	trimManagedFields := func(obj interface{}) (interface{}, error) {
		if accessor, err := meta.Accessor(obj); err == nil {
			accessor.SetManagedFields(nil)
		}
		return obj, nil
	}

	// 创建不带对象过滤条件的 informer 工厂。
	informerFactory := informers.NewSharedInformerFactoryWithOptions(s.Client, s.Config.ConfigSyncPeriod.Duration,
		informers.WithTransform(trimManagedFields),
	)

	// EndpointSlice informer 排除带有 service.kubernetes.io/headless 标签的对象。
	endpointSliceInformerFactory := informers.NewSharedInformerFactoryWithOptions(s.Client, s.Config.ConfigSyncPeriod.Duration,
		informers.WithTransform(trimManagedFields),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labelSelectorNoHeadlessEndpoints.String()
		}))

	// 注册 Service、EndpointSlice 与 ServiceCIDR 的监视。
	// 必须先注册处理器再启动 informer，否则启动时的初始更新可能因无处理器而丢失。
	// Headless Service 由 DNS 代理，不纳入 kube-proxy 监视范围。
	serviceInformerFactory := informers.NewSharedInformerFactoryWithOptions(s.Client, s.Config.ConfigSyncPeriod.Duration,
		informers.WithTransform(trimManagedFields),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labelSelectorNoProxyName.String()
			options.FieldSelector = fields.OneTermNotEqualSelector("spec.clusterIP", v1.ClusterIPNone).String()
		}))
	serviceConfig := config.NewServiceConfig(ctx, serviceInformerFactory.Core().V1().Services(), s.Config.ConfigSyncPeriod.Duration)
	serviceConfig.RegisterEventHandler(s.Proxier)
	go serviceConfig.Run(ctx.Done())

	endpointSliceConfig := config.NewEndpointSliceConfig(ctx, endpointSliceInformerFactory.Discovery().V1().EndpointSlices(), s.Config.ConfigSyncPeriod.Duration)
	endpointSliceConfig.RegisterEventHandler(s.Proxier)
	go endpointSliceConfig.Run(ctx.Done())

	if utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
		serviceCIDRConfig := config.NewServiceCIDRConfig(ctx, informerFactory.Networking().V1().ServiceCIDRs(), s.Config.ConfigSyncPeriod.Duration)
		serviceCIDRConfig.RegisterEventHandler(s.Proxier)
		go serviceCIDRConfig.Run(wait.NeverStop)
	}
	// informer 必须在 NewServiceConfig 等调用之后启动，确保共享 informer 的事件处理器已注册。
	informerFactory.Start(wait.NeverStop)
	endpointSliceInformerFactory.Start(wait.NeverStop)
	serviceInformerFactory.Start(wait.NeverStop)

	// hollow-proxy 不使用节点配置，因此没有 NodeManager，也无需启动节点 informer。
	if s.NodeManager != nil {
		nodeConfig := config.NewNodeConfig(ctx, s.NodeManager.NodeInformer(), s.Config.ConfigSyncPeriod.Duration)
		nodeConfig.RegisterEventHandler(s.NodeManager)
		nodeTopologyConfig := config.NewNodeTopologyConfig(ctx, s.NodeManager.NodeInformer(), s.Config.ConfigSyncPeriod.Duration)
		nodeTopologyConfig.RegisterEventHandler(s.Proxier)

		go nodeConfig.Run(wait.NeverStop)
	}

	// 所有启动步骤成功后发送首次运行事件。
	s.birthCry()

	go s.Proxier.SyncLoop()

	select {
	case err = <-healthzErrCh:
		s.Recorder.Eventf(s.NodeRef, nil, api.EventTypeWarning, "FailedToStartProxierHealthcheck", "StartKubeProxy", err.Error())
	case err = <-metricsErrCh:
		s.Recorder.Eventf(s.NodeRef, nil, api.EventTypeWarning, "FailedToStartMetricServer", "StartKubeProxy", err.Error())
	}
	return err
}

func (s *ProxyServer) birthCry() {
	s.Recorder.Eventf(s.NodeRef, nil, api.EventTypeNormal, "Starting", "StartKubeProxy", "")
}

func expandNodePortAddressKeywords(nodePortAddresses []string, nodeIPs map[v1.IPFamily]net.IP) []string {
	var expanded []string
	for _, addr := range nodePortAddresses {
		switch addr {
		case kubeproxyconfig.NodePortAddressesPrimary:
			// "primary" 将 NodePort 限定到节点实际 IP；回环地址不可路由，对应协议族直接跳过。
			if ip := nodeIPs[v1.IPv4Protocol]; ip != nil && !ip.IsLoopback() {
				expanded = append(expanded, fmt.Sprintf("%s/32", ip.String()))
			}
			if ip := nodeIPs[v1.IPv6Protocol]; ip != nil && !ip.IsLoopback() {
				expanded = append(expanded, fmt.Sprintf("%s/128", ip.String()))
			}
		case kubeproxyconfig.NodePortAddressesLocalhost:
			// 两种协议族都有回环地址，与节点是否拥有可路由 IP 无关。
			expanded = append(expanded, "127.0.0.0/8", "::1/128")
		case kubeproxyconfig.NodePortAddressesAll:
			expanded = append(expanded, proxyutil.IPv4ZeroCIDR, proxyutil.IPv6ZeroCIDR)
		default:
			expanded = append(expanded, addr)
		}
	}
	return expanded
}

// detectNodeIPs 返回节点 IP 与主协议族；后端不支持双栈时仅使用主协议族。
// 后端支持双栈时 kube-proxy 会启用双栈，不以节点是否实际配置双栈为前提。
//
// Linux 上节点 IP 仅用于判断 LoadBalancerSourceRanges 是否匹配本节点，不用于 NodePort。
// 地址优先级如下：
//  1. bindAddress 不是 0.0.0.0 或 :: 时，作为主 IP。
//  2. 否则使用 rawNodeIPs 中的地址。
//  3. 仍无可用地址时使用 127.0.0.1 与 ::1。
func detectNodeIPs(ctx context.Context, rawNodeIPs []net.IP, bindAddress string) (v1.IPFamily, map[v1.IPFamily]net.IP) {
	logger := klog.FromContext(ctx)
	primaryFamily := v1.IPv4Protocol
	nodeIPs := map[v1.IPFamily]net.IP{
		v1.IPv4Protocol: net.IPv4(127, 0, 0, 1),
		v1.IPv6Protocol: net.IPv6loopback,
	}

	if len(rawNodeIPs) > 0 {
		if !netutils.IsIPv4(rawNodeIPs[0]) {
			primaryFamily = v1.IPv6Protocol
		}
		nodeIPs[primaryFamily] = rawNodeIPs[0]
		if len(rawNodeIPs) > 1 {
			// 返回多个地址时，NodeManager 保证它们属于不同协议族。
			family := v1.IPv4Protocol
			if !netutils.IsIPv4(rawNodeIPs[1]) {
				family = v1.IPv6Protocol
			}
			nodeIPs[family] = rawNodeIPs[1]
		}
	}

	// 非通配 bindAddress 优先级最高，同时决定主协议族。
	bindIP := netutils.ParseIPSloppy(bindAddress)
	if bindIP != nil && !bindIP.IsUnspecified() {
		if netutils.IsIPv4(bindIP) {
			primaryFamily = v1.IPv4Protocol
		} else {
			primaryFamily = v1.IPv6Protocol
		}
		nodeIPs[primaryFamily] = bindIP
	}

	if nodeIPs[primaryFamily].IsLoopback() {
		logger.Info("Can't determine this node's IP, assuming loopback; if this is incorrect, please set the --bind-address flag")
	}
	return primaryFamily, nodeIPs
}
