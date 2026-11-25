/*
Copyright 2024 The Kubernetes Authors.

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
	"os"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/server/flagz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cliflag "k8s.io/component-base/cli/flag"
	logsapi "k8s.io/component-base/logs/api/v1"
	metricsfeatures "k8s.io/component-base/metrics/features"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/klog/v2"
	"k8s.io/kube-proxy/config/v1alpha1"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/pkg/kubelet/qos"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	proxyconfigscheme "k8s.io/kubernetes/pkg/proxy/apis/config/scheme"
	kubeproxyconfigv1alpha1 "k8s.io/kubernetes/pkg/proxy/apis/config/v1alpha1"
	"k8s.io/kubernetes/pkg/proxy/apis/config/validation"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	"k8s.io/kubernetes/pkg/util/filesystem"
	utilflag "k8s.io/kubernetes/pkg/util/flag"
	"k8s.io/utils/ptr"
)

// Options 保存创建和运行 kube-proxy 所需的命令行状态与已解析配置。
type Options struct {
	// ConfigFile 是代理服务器配置文件的路径。
	ConfigFile string
	// WriteConfigTo 是输出默认配置的路径；非空时写文件后退出。
	WriteConfigTo string
	// CleanupAndExit 为 true 时清理 iptables、ipvs 与 nftables 旧规则后退出。
	CleanupAndExit bool
	// InitAndExit 为 true 时仅执行需要特权的初始化，完成后退出。
	InitAndExit bool
	// config 是完成默认值转换后的内部配置对象。
	config *kubeproxyconfig.KubeProxyConfiguration
	// watcher 监视配置文件变化；变化后要求进程退出并由调用方重新加载配置。
	watcher filesystem.FSWatcher
	// proxyServer 是待运行的代理服务器抽象。
	proxyServer proxyRun
	// errCh 在代理、配置监视或辅助服务失败时传递退出原因。
	errCh chan error
	// flagz 为 flagz 页面提供当前命令行标志。
	flagz flagz.Reader

	// 以下字段暂存无法直接映射到 config.KubeProxyConfiguration 的已弃用标志。
	// TODO: 删除这些已弃用标志时一并移除字段。

	// master 覆盖 kubeconfig 中的 API 服务器 URL。
	master string
	// healthzPort 是已弃用的健康检查服务器端口参数。
	healthzPort int32
	// metricsPort 是已弃用的指标服务器端口参数。
	metricsPort int32

	// hostnameOverride 来自命令行时优先于配置文件中的 HostnameOverride。
	hostnameOverride string

	logger klog.Logger

	// 以下字段暂存无法直接映射到 config.KubeProxyConfiguration 的兼容标志。
	iptablesSyncPeriod    time.Duration
	iptablesMinSyncPeriod time.Duration
	ipvsSyncPeriod        time.Duration
	ipvsMinSyncPeriod     time.Duration
	clusterCIDRs          string
}

// AddFlags 向 fs 注册命令行标志，并将它们绑定到 Options 或内部配置字段。
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.addOSFlags(fs)

	fs.StringVar(&o.ConfigFile, "config", o.ConfigFile, "The path to the configuration file.")
	fs.StringVar(&o.WriteConfigTo, "write-config-to", o.WriteConfigTo, "If set, write the default configuration values to this file and exit.")

	fs.BoolVar(&o.CleanupAndExit, "cleanup", o.CleanupAndExit, "If true cleanup iptables and ipvs rules and exit.")

	fs.Var(cliflag.NewMapStringBool(&o.config.FeatureGates), "feature-gates", "A set of key=value pairs that describe feature gates for alpha/experimental features. "+
		"Options are:\n"+strings.Join(utilfeature.DefaultFeatureGate.KnownFeatures(), "\n")+"\n"+
		"This parameter is ignored if a config file is specified by --config.")

	fs.StringVar(&o.config.ClientConnection.Kubeconfig, "kubeconfig", o.config.ClientConnection.Kubeconfig, "Path to kubeconfig file with authorization information (the master location can be overridden by the master flag).")
	fs.StringVar(&o.master, "master", o.master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	fs.StringVar(&o.config.ClientConnection.ContentType, "kube-api-content-type", o.config.ClientConnection.ContentType, "Content type of requests sent to apiserver.")
	fs.Int32Var(&o.config.ClientConnection.Burst, "kube-api-burst", o.config.ClientConnection.Burst, "Burst to use while talking with kubernetes apiserver")
	fs.Float32Var(&o.config.ClientConnection.QPS, "kube-api-qps", o.config.ClientConnection.QPS, "QPS to use while talking with kubernetes apiserver")

	fs.StringVar(&o.hostnameOverride, "hostname-override", o.hostnameOverride, "If non-empty, will be used as the name of the Node that kube-proxy is running on. If unset, the node name is assumed to be the same as the node's hostname.")
	fs.Var(&utilflag.IPVar{Val: &o.config.BindAddress}, "bind-address", "Overrides kube-proxy's idea of what its node's primary IP is. Note that the name is a historical artifact, and kube-proxy does not actually bind any sockets to this IP. This parameter is ignored if a config file is specified by --config.")
	fs.Var(&utilflag.IPPortVar{Val: &o.config.HealthzBindAddress}, "healthz-bind-address", "The IP address and port for the health check server to serve on, defaulting to \"0.0.0.0:10256\". This parameter is ignored if a config file is specified by --config.")
	fs.Var(&utilflag.IPPortVar{Val: &o.config.MetricsBindAddress}, "metrics-bind-address", "The IP address and port for the metrics server to serve on, defaulting to \"127.0.0.1:10249\". (Set to \"0.0.0.0:10249\" / \"[::]:10249\" to bind on all interfaces.) This parameter is ignored if a config file is specified by --config.")
	fs.BoolVar(&o.config.BindAddressHardFail, "bind-address-hard-fail", o.config.BindAddressHardFail, "If true kube-proxy will treat failure to bind to a port as fatal and exit")
	fs.BoolVar(&o.config.EnableProfiling, "profiling", o.config.EnableProfiling, "If true enables profiling via web interface on /debug/pprof handler. This parameter is ignored if a config file is specified by --config.")
	fs.StringVar(&o.config.ShowHiddenMetricsForVersion, "show-hidden-metrics-for-version", o.config.ShowHiddenMetricsForVersion,
		"The previous version for which you want to show hidden metrics. "+
			"Only the previous minor version is meaningful, other values will not be allowed. "+
			"The format is <major>.<minor>, e.g.: '1.16'. "+
			"The purpose of this format is make sure you have the opportunity to notice if the next release hides additional metrics, "+
			"rather than being surprised when they are permanently removed in the release after that. "+
			"This parameter is ignored if a config file is specified by --config.")
	fs.BoolVar(&o.InitAndExit, "init-only", o.InitAndExit, "If true, perform any initialization steps that must be done with full root privileges, and then exit. After doing this, you can run kube-proxy again with only the CAP_NET_ADMIN capability.")
	fs.Var(&o.config.Mode, "proxy-mode", "Which proxy mode to use: on Linux this can be 'iptables' (default), 'ipvs', or 'nftables'. On Windows the only supported value is 'kernelspace'. "+
		"This parameter is ignored if a config file is specified by --config.")

	fs.Int32Var(o.config.IPTables.MasqueradeBit, "iptables-masquerade-bit", ptr.Deref(o.config.IPTables.MasqueradeBit, 14), "If using the iptables or ipvs proxy mode, the bit of the fwmark space to mark packets requiring SNAT with.  Must be within the range [0, 31].")
	fs.BoolVar(&o.config.Linux.MasqueradeAll, "masquerade-all", o.config.Linux.MasqueradeAll, "SNAT all traffic sent via Service cluster IPs. This may be required with some CNI plugins. Only supported on Linux.")
	fs.BoolVar(o.config.IPTables.LocalhostNodePorts, "iptables-localhost-nodeports", ptr.Deref(o.config.IPTables.LocalhostNodePorts, true), "If false, kube-proxy will disable the legacy behavior of allowing NodePort services to be accessed via localhost. (Applies only to iptables mode and IPv4; localhost NodePorts are never allowed with other proxy modes or with IPv6.)")
	fs.DurationVar(&o.iptablesSyncPeriod, "iptables-sync-period", o.config.SyncPeriod.Duration, "An interval (e.g. '5s', '1m', '2h22m') indicating how frequently various re-synchronizing and cleanup operations are performed. Must be greater than 0.")
	fs.DurationVar(&o.iptablesMinSyncPeriod, "iptables-min-sync-period", o.config.MinSyncPeriod.Duration, "The minimum period between iptables rule resyncs (e.g. '5s', '1m', '2h22m'). A value of 0 means every Service or EndpointSlice change will result in an immediate iptables resync.")

	fs.DurationVar(&o.ipvsSyncPeriod, "ipvs-sync-period", o.config.SyncPeriod.Duration, "An interval (e.g. '5s', '1m', '2h22m') indicating how frequently various re-synchronizing and cleanup operations are performed. Must be greater than 0.")
	fs.DurationVar(&o.ipvsMinSyncPeriod, "ipvs-min-sync-period", o.config.MinSyncPeriod.Duration, "The minimum period between IPVS rule resyncs (e.g. '5s', '1m', '2h22m'). A value of 0 means every Service or EndpointSlice change will result in an immediate IPVS resync.")
	fs.StringVar(&o.config.IPVS.Scheduler, "ipvs-scheduler", o.config.IPVS.Scheduler, "The ipvs scheduler type when proxy mode is ipvs")
	fs.StringSliceVar(&o.config.IPVS.ExcludeCIDRs, "ipvs-exclude-cidrs", o.config.IPVS.ExcludeCIDRs, "A comma-separated list of CIDRs which the ipvs proxier should not touch when cleaning up IPVS rules.")
	fs.BoolVar(&o.config.IPVS.StrictARP, "ipvs-strict-arp", o.config.IPVS.StrictARP, "Enable strict ARP by setting arp_ignore to 1 and arp_announce to 2")
	fs.DurationVar(&o.config.IPVS.TCPTimeout.Duration, "ipvs-tcp-timeout", o.config.IPVS.TCPTimeout.Duration, "The timeout for idle IPVS TCP connections, 0 to leave as-is. (e.g. '5s', '1m', '2h22m').")
	fs.DurationVar(&o.config.IPVS.TCPFinTimeout.Duration, "ipvs-tcpfin-timeout", o.config.IPVS.TCPFinTimeout.Duration, "The timeout for IPVS TCP connections after receiving a FIN packet, 0 to leave as-is. (e.g. '5s', '1m', '2h22m').")
	fs.DurationVar(&o.config.IPVS.UDPTimeout.Duration, "ipvs-udp-timeout", o.config.IPVS.UDPTimeout.Duration, "The timeout for IPVS UDP packets, 0 to leave as-is. (e.g. '5s', '1m', '2h22m').")

	fs.Var(&o.config.DetectLocalMode, "detect-local-mode", "Mode to use to detect local traffic. This parameter is ignored if a config file is specified by --config.")
	fs.StringVar(&o.config.DetectLocal.BridgeInterface, "pod-bridge-interface", o.config.DetectLocal.BridgeInterface, "A bridge interface name. When --detect-local-mode is set to BridgeInterface, kube-proxy will consider traffic to be local if it originates from this bridge.")
	fs.StringVar(&o.config.DetectLocal.InterfaceNamePrefix, "pod-interface-name-prefix", o.config.DetectLocal.InterfaceNamePrefix, "An interface name prefix. When --detect-local-mode is set to InterfaceNamePrefix, kube-proxy will consider traffic to be local if it originates from any interface whose name begins with this prefix.")
	fs.StringVar(&o.clusterCIDRs, "cluster-cidr", strings.Join(o.config.DetectLocal.ClusterCIDRs, ","), "The CIDR range of the pods in the cluster. (For dual-stack clusters, this can be a comma-separated dual-stack pair of CIDR ranges.). When --detect-local-mode is set to ClusterCIDR, kube-proxy will consider traffic to be local if its source IP is in this range. (Otherwise it is not used.) "+
		"This parameter is ignored if a config file is specified by --config.")

	fs.StringSliceVar(&o.config.NodePortAddresses, "nodeport-addresses", o.config.NodePortAddresses,
		"A list of CIDR ranges and/or keywords that expand to CIDR ranges. NodePort services are only accessible on node IPs covered by the list. Supported keywords: 'primary' (the Node object's primary IPv4 and/or IPv6 addresses), 'localhost' (127.0.0.0/8 and ::1/128), and 'all' (0.0.0.0/0 and ::/0). Any combination of valid keywords and CIDRS may be included in the list. Serving NodePorts on loopback IPs is only supported in iptables mode (IPv4 only, see --iptables-localhost-nodeports), and, only for TCP, in nftables mode when the KubeProxyNFTablesLocalhostNodePorts feature gate is enabled and the list explicitly includes loopback (e.g. 'localhost'). If unset, this defaults to 'all' in iptables and ipvs mode, and to 'primary' in nftables mode. This parameter is ignored if a config file is specified by --config.")

	fs.Int32Var(o.config.Linux.OOMScoreAdj, "oom-score-adj", ptr.Deref(o.config.Linux.OOMScoreAdj, int32(qos.KubeProxyOOMScoreAdj)), "The oom-score-adj value for kube-proxy process. Values must be within the range [-1000, 1000]. This parameter is ignored if a config file is specified by --config.")
	fs.Int32Var(o.config.Linux.Conntrack.MaxPerCore, "conntrack-max-per-core", *o.config.Linux.Conntrack.MaxPerCore,
		"Maximum number of NAT connections to track per CPU core (0 to leave the limit as-is and ignore conntrack-min).")
	fs.Int32Var(o.config.Linux.Conntrack.Min, "conntrack-min", *o.config.Linux.Conntrack.Min,
		"Minimum number of conntrack entries to allocate, regardless of conntrack-max-per-core (set conntrack-max-per-core=0 to leave the limit as-is).")

	fs.DurationVar(&o.config.Linux.Conntrack.TCPEstablishedTimeout.Duration, "conntrack-tcp-timeout-established", o.config.Linux.Conntrack.TCPEstablishedTimeout.Duration, "Idle timeout for established TCP connections (0 to leave as-is)")
	fs.DurationVar(
		&o.config.Linux.Conntrack.TCPCloseWaitTimeout.Duration, "conntrack-tcp-timeout-close-wait",
		o.config.Linux.Conntrack.TCPCloseWaitTimeout.Duration,
		"NAT timeout for TCP connections in the CLOSE_WAIT state")
	fs.BoolVar(&o.config.Linux.Conntrack.TCPBeLiberal, "conntrack-tcp-be-liberal", o.config.Linux.Conntrack.TCPBeLiberal, "Enable liberal mode for tracking TCP packets by setting nf_conntrack_tcp_be_liberal to 1")
	fs.DurationVar(&o.config.Linux.Conntrack.UDPTimeout.Duration, "conntrack-udp-timeout", o.config.Linux.Conntrack.UDPTimeout.Duration, "Idle timeout for UNREPLIED UDP connections (0 to leave as-is)")
	fs.DurationVar(&o.config.Linux.Conntrack.UDPStreamTimeout.Duration, "conntrack-udp-timeout-stream", o.config.Linux.Conntrack.UDPStreamTimeout.Duration, "Idle timeout for ASSURED UDP connections (0 to leave as-is)")
	fs.DurationVar(&o.config.ConfigSyncPeriod.Duration, "config-sync-period", o.config.ConfigSyncPeriod.Duration, "How often configuration from the apiserver is refreshed.  Must be greater than 0.")

	fs.Int32Var(&o.healthzPort, "healthz-port", o.healthzPort, "The port to bind the health check server. Use 0 to disable.")
	_ = fs.MarkDeprecated("healthz-port", "This flag is deprecated and will be removed in a future release. Please use --healthz-bind-address instead.")
	fs.Int32Var(&o.metricsPort, "metrics-port", o.metricsPort, "The port to bind the metrics server. Use 0 to disable.")
	_ = fs.MarkDeprecated("metrics-port", "This flag is deprecated and will be removed in a future release. Please use --metrics-bind-address instead.")

	logsapi.AddFlags(&o.config.Logging, fs)
}

// newKubeProxyConfiguration 先对 v1alpha1 配置应用默认值，再转换为内部类型。
func newKubeProxyConfiguration() *kubeproxyconfig.KubeProxyConfiguration {
	versionedConfig := &v1alpha1.KubeProxyConfiguration{}
	proxyconfigscheme.Scheme.Default(versionedConfig)
	internalConfig, err := proxyconfigscheme.Scheme.ConvertToVersion(versionedConfig, kubeproxyconfig.SchemeGroupVersion)
	if err != nil {
		panic(fmt.Sprintf("Unable to create default config: %v", err))
	}

	return internalConfig.(*kubeproxyconfig.KubeProxyConfiguration)
}

// NewOptions 返回已装载默认配置和兼容端口初始值的 Options。
func NewOptions() *Options {
	return &Options{
		config:      newKubeProxyConfiguration(),
		healthzPort: ports.ProxyHealthzPort,
		metricsPort: ports.ProxyStatusPort,
		errCh:       make(chan error),
		logger:      klog.FromContext(context.Background()),
	}
}

// Complete 汇总命令行、配置文件与平台默认值，使 Validate 能检查最终生效配置。
func (o *Options) Complete(fs *pflag.FlagSet) error {
	if len(o.ConfigFile) == 0 && len(o.WriteConfigTo) == 0 {
		o.config.HealthzBindAddress = addressFromDeprecatedFlags(o.config.HealthzBindAddress, o.healthzPort)
		o.config.MetricsBindAddress = addressFromDeprecatedFlags(o.config.MetricsBindAddress, o.metricsPort)
	}

	// 必须在 Complete 中加载配置文件，Validate 才能校验合并后的最终配置。
	if len(o.ConfigFile) > 0 {
		c, err := o.loadConfigFromFile(o.ConfigFile)
		if err != nil {
			return err
		}

		// 覆盖前把已修改的日志参数复制到文件配置，确保命令行日志标志优先。
		// 否则即使文件未配置日志，加载配置也会重置 `--config ... -v=5` 的详细级别。
		_ = copyLogsFromFlags(fs, &c.Logging)
		o.config = c

		if err := o.initWatcher(); err != nil {
			return err
		}
	} else {
		o.processV1Alpha1Flags(fs)
	}

	o.platformApplyDefaults(o.config)

	if err := o.processHostnameOverrideFlag(); err != nil {
		return err
	}

	if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(o.config.FeatureGates); err != nil {
		return err
	}
	// 将功能开关状态同步到指标子系统；必须晚于开关设置、早于直方图指标注册。
	metricsfeatures.ApplyFeatureGates(utilfeature.DefaultFeatureGate)

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		nfs := cliflag.NamedFlagSets{
			FlagSets: make(map[string]*pflag.FlagSet),
		}
		nfs.FlagSets["generic"] = fs
		o.flagz = flagz.NamedFlagSetsReader{
			FlagSets: nfs,
		}
	}

	return nil
}

// copyLogsFromFlags 将 from 中显式修改的日志标志复制到 to，未修改字段保持原值。
// vmodule 等可重复字段由标志值追加在前，确保命令行配置优先。
// TODO(pohly): 移到 logsapi。
func copyLogsFromFlags(from *pflag.FlagSet, to *logsapi.LoggingConfiguration) error {
	var cloneFS pflag.FlagSet
	logsapi.AddFlags(to, &cloneFS)
	vmodule := to.VModule
	to.VModule = nil
	var err error
	cloneFS.VisitAll(func(f *pflag.Flag) {
		if err != nil {
			return
		}
		fsFlag := from.Lookup(f.Name)
		if fsFlag == nil {
			err = fmt.Errorf("logging flag %s not found in flag set", f.Name)
			return
		}
		if !fsFlag.Changed {
			return
		}
		if setErr := f.Value.Set(fsFlag.Value.String()); setErr != nil {
			err = fmt.Errorf("copying flag %s value: %w", f.Name, setErr)
			return
		}
	})
	to.VModule = append(to.VModule, vmodule...)
	return err
}

// initWatcher 创建文件系统监视器并监听配置文件。
func (o *Options) initWatcher() error {
	fswatcher := filesystem.NewFsnotifyWatcher()
	err := fswatcher.Init(o.eventHandler, o.errorHandler)
	if err != nil {
		return err
	}
	err = fswatcher.AddWatch(o.ConfigFile)
	if err != nil {
		return err
	}
	o.watcher = fswatcher
	return nil
}

func (o *Options) eventHandler(ent fsnotify.Event) {
	if ent.Has(fsnotify.Write) || ent.Has(fsnotify.Rename) {
		// 配置文件更新后不热加载，发送错误要求进程退出并重新读取。
		o.errCh <- fmt.Errorf("content of the proxy server's configuration file was updated")
		return
	}
	o.errCh <- nil
}

func (o *Options) errorHandler(err error) {
	o.errCh <- err
}

// processHostnameOverrideFlag 处理 hostname-override 标志，并拒绝仅含空白的值。
func (o *Options) processHostnameOverrideFlag() error {
	// 命令行标志非空时覆盖配置文件值，并统一去除空白、转小写。
	if len(o.hostnameOverride) > 0 {
		hostName := strings.TrimSpace(o.hostnameOverride)
		if len(hostName) == 0 {
			return fmt.Errorf("empty hostname-override is invalid")
		}
		o.config.HostnameOverride = strings.ToLower(hostName)
	}

	return nil
}

// processV1Alpha1Flags 将无法直接映射到内部配置的 v1alpha1 兼容标志合并进去。
func (o *Options) processV1Alpha1Flags(fs *pflag.FlagSet) {
	if fs.Changed("iptables-sync-period") && o.config.Mode != kubeproxyconfig.ProxyModeIPVS {
		o.config.SyncPeriod.Duration = o.iptablesSyncPeriod
	}
	if fs.Changed("iptables-min-sync-period") && o.config.Mode != kubeproxyconfig.ProxyModeIPVS {
		o.config.MinSyncPeriod.Duration = o.iptablesMinSyncPeriod
	}
	if fs.Changed("ipvs-sync-period") && o.config.Mode == kubeproxyconfig.ProxyModeIPVS {
		o.config.SyncPeriod.Duration = o.ipvsSyncPeriod
	}
	if fs.Changed("ipvs-min-sync-period") && o.config.Mode == kubeproxyconfig.ProxyModeIPVS {
		o.config.MinSyncPeriod.Duration = o.ipvsMinSyncPeriod
	}
	if fs.Changed("cluster-cidr") {
		o.config.DetectLocal.ClusterCIDRs = strings.Split(o.clusterCIDRs, ",")
	}
}

// Validate 校验完成合并的内部配置，并汇总所有字段错误。
func (o *Options) Validate() error {
	if errs := validation.Validate(o.config); len(errs) != 0 {
		return errs.ToAggregate()
	}

	return nil
}

// Run 写出默认配置、执行清理或初始化，否则启动代理并等待退出。
func (o *Options) Run(ctx context.Context) error {
	defer close(o.errCh)
	if len(o.WriteConfigTo) > 0 {
		return o.writeConfigFile()
	}

	err := platformCleanup(ctx, o.config.Mode, o.CleanupAndExit)
	if o.CleanupAndExit {
		return err
	}
	// 普通启动时清理只是尽力而为；后端会记录有意义的失败，因此这里不中止启动。

	proxyServer, err := newProxyServer(ctx, o.config, o.master, o.InitAndExit, o.flagz)
	if err != nil {
		return err
	}
	if o.InitAndExit {
		return nil
	}

	o.proxyServer = proxyServer
	return o.runLoop(ctx)
}

// runLoop 在后台运行代理，并等待代理或配置文件监视器报告结果。
// 任一非 nil 错误都会结束循环并向上返回。
func (o *Options) runLoop(ctx context.Context) error {
	if o.watcher != nil {
		o.watcher.Run(ctx)
	}

	// 在 goroutine 中运行代理，使主循环能同时接收配置监视事件。
	go func() {
		err := o.proxyServer.Run(ctx)
		o.errCh <- err
	}()

	for {
		err := <-o.errCh
		if err != nil {
			return err
		}
	}
}

func (o *Options) writeConfigFile() (err error) {
	const mediaType = runtime.ContentTypeYAML
	info, ok := runtime.SerializerInfoForMediaType(proxyconfigscheme.Codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}

	encoder := proxyconfigscheme.Codecs.EncoderForVersion(info.Serializer, v1alpha1.SchemeGroupVersion)

	configFile, err := os.Create(o.WriteConfigTo)
	if err != nil {
		return err
	}

	defer func() {
		ferr := configFile.Close()
		if ferr != nil && err == nil {
			err = ferr
		}
	}()

	if err = encoder.Encode(o.config, configFile); err != nil {
		return err
	}

	o.logger.Info("Wrote configuration", "file", o.WriteConfigTo)

	return nil
}

// addressFromDeprecatedFlags 根据已弃用的独立端口参数生成监听地址。
// port 为 0 时返回空地址以禁用服务；否则仅替换 addr 的端口部分。
func addressFromDeprecatedFlags(addr string, port int32) string {
	if port == 0 {
		return ""
	}
	return proxyutil.AppendPortIfNeeded(addr, port)
}

// newLenientSchemeAndCodecs 创建注册内部配置与 v1alpha1、且关闭严格解码的 Scheme 与 CodecFactory。
// 宽松路径仅用于兼容 v1alpha1，避免意外接受其它配置 API。
func newLenientSchemeAndCodecs() (*runtime.Scheme, *serializer.CodecFactory, error) {
	lenientScheme := runtime.NewScheme()
	if err := kubeproxyconfig.AddToScheme(lenientScheme); err != nil {
		return nil, nil, fmt.Errorf("failed to add kube-proxy config API to lenient scheme: %w", err)
	}
	if err := kubeproxyconfigv1alpha1.AddToScheme(lenientScheme); err != nil {
		return nil, nil, fmt.Errorf("failed to add kube-proxy config v1alpha1 API to lenient scheme: %w", err)
	}
	lenientCodecs := serializer.NewCodecFactory(lenientScheme, serializer.DisableStrict)
	return lenientScheme, &lenientCodecs, nil
}

// loadConfigFromFile 读取文件并将内容解码为 KubeProxyConfiguration。
func (o *Options) loadConfigFromFile(file string) (*kubeproxyconfig.KubeProxyConfiguration, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return o.loadConfig(data)
}

// loadConfig 将序列化的 KubeProxyConfiguration 解码为内部类型。
// 严格解码失败时，仅 v1alpha1 可回退到宽松解码并记录警告。
func (o *Options) loadConfig(data []byte) (*kubeproxyconfig.KubeProxyConfiguration, error) {

	configObj, gvk, err := proxyconfigscheme.Codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		// 先严格解码；失败后仅对 v1alpha1 使用宽松解码并记录警告。
		// 停止支持 v1alpha1 时必须移除该兼容路径。
		if !runtime.IsStrictDecodingError(err) {
			return nil, fmt.Errorf("failed to decode: %w", err)
		}

		_, lenientCodecs, lenientErr := newLenientSchemeAndCodecs()
		if lenientErr != nil {
			return nil, lenientErr
		}

		configObj, gvk, lenientErr = lenientCodecs.UniversalDecoder().Decode(data, nil, nil)
		if lenientErr != nil {
			// 当前版本宽松解码也失败时，返回最初的严格解码错误。
			return nil, fmt.Errorf("failed lenient decoding: %w", err)
		}

		// 保留宽松解码得到的 v1alpha1 对象，同时把降级事实写入日志。
		o.logger.Info("Using lenient decoding as strict decoding failed", "err", err)
	}

	proxyConfig, ok := configObj.(*kubeproxyconfig.KubeProxyConfiguration)
	if !ok {
		return nil, fmt.Errorf("got unexpected config type: %v", gvk)
	}
	return proxyConfig, nil
}
