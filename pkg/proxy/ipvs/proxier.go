//go:build linux

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

package ipvs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/events"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/conntrack"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	utilipset "k8s.io/kubernetes/pkg/proxy/ipvs/ipset"
	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
	"k8s.io/kubernetes/pkg/proxy/metaproxier"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/kubernetes/pkg/proxy/runner"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
	utilkernel "k8s.io/kubernetes/pkg/util/kernel"
	netutils "k8s.io/utils/net"
)

const (
	// kubeServicesChain 是 Service 的访问入口链（services portal chain）。
	kubeServicesChain utiliptables.Chain = "KUBE-SERVICES"

	// kubeProxyFirewallChain 是 kube-proxy 的防火墙链。
	kubeProxyFirewallChain utiliptables.Chain = "KUBE-PROXY-FIREWALL"

	// kubeSourceRangesFirewallChain 是 LoadBalancerSourceRanges 使用的防火墙子链。
	kubeSourceRangesFirewallChain utiliptables.Chain = "KUBE-SOURCE-RANGES-FIREWALL"

	// kubePostroutingChain 是 Kubernetes 的 postrouting 链。
	kubePostroutingChain utiliptables.Chain = "KUBE-POSTROUTING"

	// kubeMarkMasqChain 用于给需要伪装（masquerade）的数据包打标记。
	kubeMarkMasqChain utiliptables.Chain = "KUBE-MARK-MASQ"

	// kubeNodePortChain 是 Kubernetes 的 NodePort 链。
	kubeNodePortChain utiliptables.Chain = "KUBE-NODE-PORT"

	// kubeForwardChain 是 Kubernetes 的 FORWARD 链。
	kubeForwardChain utiliptables.Chain = "KUBE-FORWARD"

	// kubeLoadBalancerChain 是 Kubernetes 为 LoadBalancer 类型 Service 创建的链。
	kubeLoadBalancerChain utiliptables.Chain = "KUBE-LOAD-BALANCER"

	// kubeIPVSFilterChain 用于过滤从外部访问主机主网络命名空间的行为。
	// https://github.com/kubernetes/kubernetes/issues/72236
	kubeIPVSFilterChain utiliptables.Chain = "KUBE-IPVS-FILTER"

	// kubeIPVSOutFilterChain 用于过滤从节点访问 LoadBalancer Service 的行为。
	// https://github.com/kubernetes/kubernetes/issues/119656
	kubeIPVSOutFilterChain utiliptables.Chain = "KUBE-IPVS-OUT-FILTER"

	// defaultScheduler 是默认的 IPVS 调度算法：轮询（round robin）。
	defaultScheduler = "rr"

	// defaultDummyDevice 是默认的 dummy 网卡，IPVS 的 Service 地址会绑定到它上面。
	defaultDummyDevice = "kube-ipvs0"
)

// 在 IPVS 代理模式下，需要设置以下 sysctl 参数。
const (
	sysctlVSConnTrack             = "net/ipv4/vs/conntrack"
	sysctlConnReuse               = "net/ipv4/vs/conn_reuse_mode"
	sysctlExpireNoDestConn        = "net/ipv4/vs/expire_nodest_conn"
	sysctlExpireQuiescentTemplate = "net/ipv4/vs/expire_quiescent_template"
	sysctlForward                 = "net/ipv4/ip_forward"
	sysctlArpIgnore               = "net/ipv4/conf/all/arp_ignore"
	sysctlArpAnnounce             = "net/ipv4/conf/all/arp_announce"
)

// NewDualStackProxier 返回一个用于双栈（dual-stack）场景的 Proxier。
func NewDualStackProxier(
	ctx context.Context,
	config *kubeproxyconfig.KubeProxyConfiguration,
	ipts map[v1.IPFamily]utiliptables.Interface,
	ipvs utilipvs.Interface,
	ipset utilipset.Interface,
	sysctl utilsysctl.Interface,
	localDetectors map[v1.IPFamily]proxyutil.LocalTrafficDetector,
	nodeName string,
	nodeIPs map[v1.IPFamily]net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
	initOnly bool,
) (proxy.Provider, error) {
	// 先创建 IPv4 的单栈 Proxier 实例。
	ipv4Proxier, err := NewProxier(ctx, config, v1.IPv4Protocol, ipts[v1.IPv4Protocol], ipvs, ipset, sysctl,
		localDetectors[v1.IPv4Protocol], nodeName, nodeIPs[v1.IPv4Protocol], recorder,
		healthzServer, initOnly)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv4 proxier: %v", err)
	}

	ipv6Proxier, err := NewProxier(ctx, config, v1.IPv6Protocol, ipts[v1.IPv6Protocol], ipvs, ipset, sysctl,
		localDetectors[v1.IPv6Protocol], nodeName, nodeIPs[v1.IPv6Protocol], recorder,
		healthzServer, initOnly)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv6 proxier: %v", err)
	}
	// --init-only 只完成系统级初始化（sysctl 与超时），不构建 Proxier 对象。
	if initOnly {
		return nil, nil
	}

	// 返回一个 meta-proxier，由它在两个单栈 Proxier 实例之间分发调用。
	// 两个实例分别负责一个 IP 协议族。
	return metaproxier.NewMetaProxier(ipv4Proxier, ipv6Proxier), nil
}

// Proxier 是基于 IPVS 实现的代理。
type Proxier struct {
	// ipFamily 是该代理所服务的 IP 协议族。
	ipFamily v1.IPFamily
	// endpointsChanges 和 serviceChanges 保存自上次 syncProxyRules 以来，
	// Endpoint 与 Service 发生的全部变更。对单个对象而言，变更会被累积：
	// previous 是这一批变更发生之前的状态，
	// current 是应用全部变更之后的状态。
	endpointsChanges *proxy.EndpointsChangeTracker
	serviceChanges   *proxy.ServiceChangeTracker

	mu             sync.Mutex // 保护以下字段。
	svcPortMap     proxy.ServicePortMap
	endpointsMap   proxy.EndpointsMap
	topologyLabels map[string]string
	// initialSync 表示 Proxier 当前是否在进行首次同步。
	// 新建 Proxier 时置为 true，之后的所有同步都会将其置为 false。
	// （即只有第一次同步时该标志为 true。）
	// 这样可以执行一些仅在代理启动阶段需要的特殊逻辑。
	// 例如，只在启动时更新已有 RealServer 的权重，
	// 避免每次同步都查询并更新 RealServer，从而节省开销。
	initialSync bool
	// endpointSlicesSynced 与 servicesSynced 在启动后对应对象
	// 完成同步时置为 true，用于避免 kube-proxy 重启后
	// 使用不完整的数据更新 IPVS 规则。
	endpointSlicesSynced bool
	servicesSynced       bool
	initialized          int32
	syncRunner           *runner.BoundedFrequencyRunner // governs calls to syncProxyRules

	// 以下字段实际上是常量，访问时无需持有互斥锁。
	syncPeriod    time.Duration
	minSyncPeriod time.Duration
	// excludeCIDRs 是清理 IPVS 规则时需要排除的 CIDR 列表。
	excludeCIDRs []*net.IPNet

	iptables       utiliptables.Interface
	ipvs           utilipvs.Interface
	ipset          utilipset.Interface
	conntrack      conntrack.Interface
	masqueradeAll  bool
	masqueradeMark string
	localDetector  proxyutil.LocalTrafficDetector
	nodeName       string
	nodeIP         net.IP

	serviceHealthServer healthcheck.ServiceHealthServer
	healthzServer       *healthcheck.ProxyHealthServer

	ipvsScheduler string
	// 以下缓冲区用于复用内存，避免频繁分配，
	// 否则会对性能造成明显影响。
	iptablesData     *bytes.Buffer
	filterChainsData *bytes.Buffer
	natChains        proxyutil.LineBuffer
	filterChains     proxyutil.LineBuffer
	natRules         proxyutil.LineBuffer
	filterRules      proxyutil.LineBuffer
	// 将其作为结构体成员，便于在测试中注入替身实现。
	netlinkHandle NetLinkHandle
	// ipsetList 是 IPVS Proxier 使用的 ipset 集合列表。
	ipsetList map[string]*IPSet
	// nodePortAddresses 用于选择 NodePort 生效的网卡。
	nodePortAddresses *proxyutil.NodePortAddresses
	// networkInterfacer 封装了若干 net 库函数的接口，
	// 注入它以便测试。
	networkInterfacer     proxyutil.NetworkInterfacer
	gracefuldeleteManager *GracefulTerminationManager
	// serviceNoLocalEndpointsInternal 记录因 internalTrafficPolicy 为 "Local" 但缺少
	// 本地 Endpoint 而无法生效的 Service 集合。
	// 它用于上报 sync_proxy_rules_no_endpoints_total 指标，
	// 其中 traffic_policy 标签取值为 "internal"。
	// 使用 Set 是因为同一个 Service 有多个端口时会被多次计算 Endpoint 拓扑，
	// 但每个 Service 在指标中只应被统计一次。
	serviceNoLocalEndpointsInternal sets.Set[string]
	// serviceNoLocalEndpointsExternal 记录因 externalTrafficPolicy 为 "Local" 但缺少
	// 任何 Endpoint 而无法生效的 Service 集合。
	// 它用于上报 sync_proxy_rules_no_endpoints_total 指标，
	// 其中 traffic_policy 标签取值为 "external"。
	// 使用 Set 是因为同一个 Service 有多个端口时会被多次计算 Endpoint 拓扑，
	// 但每个 Service 在指标中只应被统计一次。
	serviceNoLocalEndpointsExternal sets.Set[string]
	// lbNoNodeAccessIPPortProtocolEntries 记录不允许从 K8s 节点访问的 LoadBalancer IP+端口+协议组合。
	// LoadBalancerSourceRanges 无法直接限制来自节点的访问，因此需要额外安装
	// iptables 规则。
	// (ref: https://github.com/kubernetes/kubernetes/issues/119656)
	lbNoNodeAccessIPPortProtocolEntries []*utilipset.Entry

	logger klog.Logger
}

// Proxier 实现了 proxy.Provider 接口。
var _ proxy.Provider = &Proxier{}

// NewProxier 返回一个单栈 IPVS Proxier。
func NewProxier(
	ctx context.Context,
	config *kubeproxyconfig.KubeProxyConfiguration,
	ipFamily v1.IPFamily,
	ipt utiliptables.Interface,
	ipvs utilipvs.Interface,
	ipset utilipset.Interface,
	sysctl utilsysctl.Interface,
	localDetector proxyutil.LocalTrafficDetector,
	nodeName string,
	nodeIP net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
	initOnly bool,
) (*Proxier, error) {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "ipFamily", ipFamily)
	// 设置 IPVS 所需的 conntrack sysctl 参数。
	// 连接跟踪是 conntrack 清理与 NAT 逻辑的基础，必须先开启。
	if err := proxyutil.EnsureSysctl(sysctl, sysctlVSConnTrack, 1); err != nil {
		return nil, err
	}

	kernelVersion, err := utilkernel.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get kernel version: %w", err)
	}

	// conn_reuse_mode 在不同内核版本上行为不同，需要按版本分别处理。
	if kernelVersion.LessThan(version.MustParseGeneric(utilkernel.IPVSConnReuseModeMinSupportedKernelVersion)) {
		logger.Error(nil, "Can't set sysctl, kernel version doesn't satisfy minimum version requirements", "sysctl", sysctlConnReuse, "minimumKernelVersion", utilkernel.IPVSConnReuseModeMinSupportedKernelVersion)
	} else if kernelVersion.AtLeast(version.MustParseGeneric(utilkernel.IPVSConnReuseModeFixedKernelVersion)) {
		// https://github.com/kubernetes/kubernetes/issues/93297
		logger.V(2).Info("Left as-is", "sysctl", sysctlConnReuse)
	} else {
		// 设置连接复用模式。
		if err := proxyutil.EnsureSysctl(sysctl, sysctlConnReuse, 0); err != nil {
			return nil, err
		}
	}

	// 设置 expire_nodest_conn，使后端消失后能尽快断开旧连接。
	if err := proxyutil.EnsureSysctl(sysctl, sysctlExpireNoDestConn, 1); err != nil {
		return nil, err
	}

	// 设置 expire_quiescent_template，使权重为 0 的后端连接能被及时清理。
	if err := proxyutil.EnsureSysctl(sysctl, sysctlExpireQuiescentTemplate, 1); err != nil {
		return nil, err
	}

	// 开启 IP 转发，IPVS 才能把报文转发给后端。
	if err := proxyutil.EnsureSysctl(sysctl, sysctlForward, 1); err != nil {
		return nil, err
	}

	// StrictARP 模式下必须调整 ARP 参数，避免其他节点抢答 Service IP 的 ARP 请求。
	if config.IPVS.StrictARP {
		// 设置 arp_ignore，避免节点响应不属于自己的 VIP 的 ARP 请求。
		if err := proxyutil.EnsureSysctl(sysctl, sysctlArpIgnore, 1); err != nil {
			return nil, err
		}

		// 设置 arp_announce，使 ARP 应答使用正确的源地址。
		if err := proxyutil.EnsureSysctl(sysctl, sysctlArpAnnounce, 2); err != nil {
			return nil, err
		}
	}

	// 只要设置了任意一个超时参数，就统一配置 IPVS 的超时时间。
	// 效果等同于执行 ipvsadm --set；取值为 0 表示保留
	// 系统当前的超时设置。
	tcpTimeout := config.IPVS.TCPTimeout.Duration
	tcpFinTimeout := config.IPVS.TCPFinTimeout.Duration
	udpTimeout := config.IPVS.UDPTimeout.Duration
	// 三个超时值中任意一个非零才需要调用 ConfigureTimeouts；0 代表沿用系统默认值。
	if tcpTimeout > 0 || tcpFinTimeout > 0 || udpTimeout > 0 {
		if err := ipvs.ConfigureTimeouts(tcpTimeout, tcpFinTimeout, udpTimeout); err != nil {
			logger.Error(err, "Failed to configure IPVS timeouts")
		}
	}

	// --init-only 只完成系统级初始化（sysctl 与超时），不构建 Proxier 对象。
	if initOnly {
		logger.Info("System initialized and --init-only specified")
		return nil, nil
	}

	// 生成供 SNAT 规则使用的 masquerade 标记。
	masqueradeValue := 1 << uint(*config.IPTables.MasqueradeBit)
	masqueradeMark := fmt.Sprintf("%#08x", masqueradeValue)

	logger.V(2).Info("Record nodeIP and family", "nodeIP", nodeIP, "family", ipFamily)

	scheduler := config.IPVS.Scheduler
	// 用户未指定调度算法时回退到默认的轮询算法。
	if len(scheduler) == 0 {
		logger.Info("IPVS scheduler not specified, use rr by default")
		scheduler = defaultScheduler
	}

	nodePortAddresses := proxyutil.NewNodePortAddresses(ipFamily, config.NodePortAddresses)

	serviceHealthServer := healthcheck.NewServiceHealthServer(nodeName, recorder, nodePortAddresses, healthzServer, ipFamily)

	syncPeriod := config.SyncPeriod.Duration
	minSyncPeriod := config.MinSyncPeriod.Duration

	excludeCIDRs := filterCIDRs(ipFamily == v1.IPv6Protocol, config.IPVS.ExcludeCIDRs)
	parsedExcludeCIDRs, _ := netutils.ParseCIDRs(excludeCIDRs)

	// initialSync 初始为 true，仅首次同步会重置已有 RealServer 的权重。
	proxier := &Proxier{
		ipFamily:              ipFamily,
		svcPortMap:            make(proxy.ServicePortMap),
		serviceChanges:        proxy.NewServiceChangeTracker(ipFamily, newServiceInfo, nil),
		endpointsMap:          make(proxy.EndpointsMap),
		endpointsChanges:      proxy.NewEndpointsChangeTracker(ipFamily, nodeName, nil, nil),
		initialSync:           true,
		syncPeriod:            syncPeriod,
		minSyncPeriod:         minSyncPeriod,
		excludeCIDRs:          parsedExcludeCIDRs,
		iptables:              ipt,
		masqueradeAll:         config.Linux.MasqueradeAll,
		masqueradeMark:        masqueradeMark,
		conntrack:             conntrack.New(),
		localDetector:         localDetector,
		nodeName:              nodeName,
		nodeIP:                nodeIP,
		serviceHealthServer:   serviceHealthServer,
		healthzServer:         healthzServer,
		ipvs:                  ipvs,
		ipvsScheduler:         scheduler,
		iptablesData:          bytes.NewBuffer(nil),
		filterChainsData:      bytes.NewBuffer(nil),
		natChains:             proxyutil.NewLineBuffer(),
		natRules:              proxyutil.NewLineBuffer(),
		filterChains:          proxyutil.NewLineBuffer(),
		filterRules:           proxyutil.NewLineBuffer(),
		netlinkHandle:         NewNetLinkHandle(ipFamily == v1.IPv6Protocol),
		ipset:                 ipset,
		nodePortAddresses:     nodePortAddresses,
		networkInterfacer:     proxyutil.RealNetwork{},
		gracefuldeleteManager: NewGracefulTerminationManager(ipvs),
		logger:                logger,
	}
	// 用所需的全部 ipset 初始化 ipsetList。
	proxier.ipsetList = make(map[string]*IPSet)
	for _, is := range ipsetInfo {
		proxier.ipsetList[is.name] = NewIPSet(ipset, is.name, is.setType, (ipFamily == v1.IPv6Protocol), is.comment)
	}

	logger.V(2).Info("ipvs sync params", "minSyncPeriod", minSyncPeriod, "syncPeriod", syncPeriod, "maxSyncPeriod", syncPeriod)
	proxier.syncRunner = runner.NewBoundedFrequencyRunner("sync-runner", proxier.syncProxyRules, minSyncPeriod, syncPeriod, syncPeriod)

	// 启动优雅删除管理器，由它异步处理处于 termination 状态的 RealServer。
	proxier.gracefuldeleteManager.Run()
	return proxier, nil
}

// filterCIDRs 从 CIDR 列表中筛出与指定 IP 协议族匹配的条目。
func filterCIDRs(wantIPv6 bool, cidrs []string) []string {
	var filteredCIDRs []string
	for _, cidr := range cidrs {
		if netutils.IsIPv6CIDRString(cidr) == wantIPv6 {
			filteredCIDRs = append(filteredCIDRs, cidr)
		}
	}
	return filteredCIDRs
}

// iptablesJumpChain 记录了 IPVS Proxier 安装或清理 iptables 规则时涉及的链跳转关系。
// `to` 是目标 iptables 链。
// `from` 是源 iptables 链。
var iptablesJumpChain = []struct {
	table   utiliptables.Table
	from    utiliptables.Chain
	to      utiliptables.Chain
	comment string
}{
	{utiliptables.TableNAT, utiliptables.ChainOutput, kubeServicesChain, "kubernetes service portals"},
	{utiliptables.TableNAT, utiliptables.ChainPrerouting, kubeServicesChain, "kubernetes service portals"},
	{utiliptables.TableNAT, utiliptables.ChainPostrouting, kubePostroutingChain, "kubernetes postrouting rules"},
	{utiliptables.TableFilter, utiliptables.ChainForward, kubeForwardChain, "kubernetes forwarding rules"},
	{utiliptables.TableFilter, utiliptables.ChainInput, kubeNodePortChain, "kubernetes health check rules"},
	{utiliptables.TableFilter, utiliptables.ChainInput, kubeProxyFirewallChain, "kube-proxy firewall rules"},
	{utiliptables.TableFilter, utiliptables.ChainForward, kubeProxyFirewallChain, "kube-proxy firewall rules"},
	{utiliptables.TableFilter, utiliptables.ChainInput, kubeIPVSFilterChain, "kubernetes ipvs access filter"},
	{utiliptables.TableFilter, utiliptables.ChainOutput, kubeIPVSOutFilterChain, "kubernetes ipvs access filter"},
}

// iptablesChains 列出需要创建并写入 iptables-restore 输入的 kube 链。
var iptablesChains = []struct {
	table utiliptables.Table
	chain utiliptables.Chain
}{
	{utiliptables.TableNAT, kubeServicesChain},
	{utiliptables.TableNAT, kubePostroutingChain},
	{utiliptables.TableNAT, kubeNodePortChain},
	{utiliptables.TableNAT, kubeLoadBalancerChain},
	{utiliptables.TableNAT, kubeMarkMasqChain},
	{utiliptables.TableFilter, kubeForwardChain},
	{utiliptables.TableFilter, kubeNodePortChain},
	{utiliptables.TableFilter, kubeProxyFirewallChain},
	{utiliptables.TableFilter, kubeSourceRangesFirewallChain},
	{utiliptables.TableFilter, kubeIPVSFilterChain},
	{utiliptables.TableFilter, kubeIPVSOutFilterChain},
}

// iptablesCleanupChains 列出清理阶段需要处理的 kube 链。
var iptablesCleanupChains = []struct {
	table utiliptables.Table
	chain utiliptables.Chain
}{
	{utiliptables.TableNAT, kubeServicesChain},
	{utiliptables.TableNAT, kubePostroutingChain},
	{utiliptables.TableNAT, kubeNodePortChain},
	{utiliptables.TableNAT, kubeLoadBalancerChain},
	{utiliptables.TableNAT, kubeMarkMasqChain},
	{utiliptables.TableFilter, kubeForwardChain},
	{utiliptables.TableFilter, kubeNodePortChain},
	{utiliptables.TableFilter, kubeProxyFirewallChain},
	{utiliptables.TableFilter, kubeSourceRangesFirewallChain},
	{utiliptables.TableFilter, kubeIPVSFilterChain},
	{utiliptables.TableFilter, kubeIPVSOutFilterChain},
}

// ipsetInfo 列出 IPVS Proxier 所需的全部 ipset 及其类型和注释。
var ipsetInfo = []struct {
	name    string
	setType utilipset.Type
	comment string
}{
	{kubeLoopBackIPSet, utilipset.HashIPPortIP, kubeLoopBackIPSetComment},
	{kubeClusterIPSet, utilipset.HashIPPort, kubeClusterIPSetComment},
	{kubeExternalIPSet, utilipset.HashIPPort, kubeExternalIPSetComment},
	{kubeExternalIPLocalSet, utilipset.HashIPPort, kubeExternalIPLocalSetComment},
	{kubeLoadBalancerSet, utilipset.HashIPPort, kubeLoadBalancerSetComment},
	{kubeLoadBalancerFWSet, utilipset.HashIPPort, kubeLoadBalancerFWSetComment},
	{kubeLoadBalancerLocalSet, utilipset.HashIPPort, kubeLoadBalancerLocalSetComment},
	{kubeLoadBalancerSourceIPSet, utilipset.HashIPPortIP, kubeLoadBalancerSourceIPSetComment},
	{kubeLoadBalancerSourceCIDRSet, utilipset.HashIPPortNet, kubeLoadBalancerSourceCIDRSetComment},
	{kubeNodePortSetTCP, utilipset.BitmapPort, kubeNodePortSetTCPComment},
	{kubeNodePortLocalSetTCP, utilipset.BitmapPort, kubeNodePortLocalSetTCPComment},
	{kubeNodePortSetUDP, utilipset.BitmapPort, kubeNodePortSetUDPComment},
	{kubeNodePortLocalSetUDP, utilipset.BitmapPort, kubeNodePortLocalSetUDPComment},
	{kubeNodePortSetSCTP, utilipset.HashIPPort, kubeNodePortSetSCTPComment},
	{kubeNodePortLocalSetSCTP, utilipset.HashIPPort, kubeNodePortLocalSetSCTPComment},
	{kubeHealthCheckNodePortSet, utilipset.BitmapPort, kubeHealthCheckNodePortSetComment},
	{kubeIPVSSet, utilipset.HashIP, kubeIPVSSetComment},
}

// ipsetWithIptablesChain 定义了 ipset 与 iptables 源链、目标跳链之间的对应关系。
// `iptables -t nat -A <from> -m set --match-set <name> <matchType> -j <to>`
// 示例：iptables -t nat -A KUBE-SERVICES -m set --match-set KUBE-NODE-PORT-TCP dst -j KUBE-NODE-PORT
// 需要其他匹配规则的 ipset 会单独创建。
// 注意：kubeNodePortLocalSetTCP 必须排在 kubeNodePortSetTCP 之前，UDP 同理；
// 这样本地流量先命中 RETURN，而不会被后续的 MASQUERADE 规则改写。
var ipsetWithIptablesChain = []struct {
	name          string
	table         utiliptables.Table
	from          string
	to            string
	matchType     string
	protocolMatch string
}{
	{kubeLoopBackIPSet, utiliptables.TableNAT, string(kubePostroutingChain), "MASQUERADE", "dst,dst,src", ""},
	{kubeLoadBalancerSet, utiliptables.TableNAT, string(kubeServicesChain), string(kubeLoadBalancerChain), "dst,dst", ""},
	{kubeLoadBalancerLocalSet, utiliptables.TableNAT, string(kubeLoadBalancerChain), "RETURN", "dst,dst", ""},
	{kubeNodePortLocalSetTCP, utiliptables.TableNAT, string(kubeNodePortChain), "RETURN", "dst", utilipset.ProtocolTCP},
	{kubeNodePortSetTCP, utiliptables.TableNAT, string(kubeNodePortChain), string(kubeMarkMasqChain), "dst", utilipset.ProtocolTCP},
	{kubeNodePortLocalSetUDP, utiliptables.TableNAT, string(kubeNodePortChain), "RETURN", "dst", utilipset.ProtocolUDP},
	{kubeNodePortSetUDP, utiliptables.TableNAT, string(kubeNodePortChain), string(kubeMarkMasqChain), "dst", utilipset.ProtocolUDP},
	{kubeNodePortLocalSetSCTP, utiliptables.TableNAT, string(kubeNodePortChain), "RETURN", "dst,dst", utilipset.ProtocolSCTP},
	{kubeNodePortSetSCTP, utiliptables.TableNAT, string(kubeNodePortChain), string(kubeMarkMasqChain), "dst,dst", utilipset.ProtocolSCTP},

	{kubeLoadBalancerFWSet, utiliptables.TableFilter, string(kubeProxyFirewallChain), string(kubeSourceRangesFirewallChain), "dst,dst", ""},
	{kubeLoadBalancerSourceCIDRSet, utiliptables.TableFilter, string(kubeSourceRangesFirewallChain), "RETURN", "dst,dst,src", ""},
	{kubeLoadBalancerSourceIPSet, utiliptables.TableFilter, string(kubeSourceRangesFirewallChain), "RETURN", "dst,dst,src", ""},
}

// servicePortInfo 是 ServicePort 的内部实现，缓存了字符串形式的名称以提升性能。
type servicePortInfo struct {
	*proxy.BaseServicePortInfo
	// 以下字段提前计算并缓存，避免热路径上的重复计算。
	nameString string
}

// newServiceInfo 返回一个封装 serviceInfo 的 proxy.ServicePort。
func newServiceInfo(port *v1.ServicePort, service *v1.Service, bsvcPortInfo *proxy.BaseServicePortInfo) proxy.ServicePort {
	svcPort := &servicePortInfo{BaseServicePortInfo: bsvcPortInfo}

	// 提前计算并保存，避免后续反复拼接名称字符串。
	svcName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	svcPortName := proxy.ServicePortName{NamespacedName: svcName, Port: port.Name}
	svcPort.nameString = svcPortName.String()

	return svcPort
}

// getFirstColumn 读取 r 的全部内容，并返回由每行第一个字段组成的切片。
// 每行按空白切分后取第一个词；调用方需确保 r 的内容规模可控。
func getFirstColumn(r io.Reader) ([]string, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(b), "\n")
	words := make([]string, 0, len(lines))
	for i := range lines {
		fields := strings.Fields(lines[i])
		if len(fields) > 0 {
			words = append(words, fields[0])
		}
	}
	return words, nil
}

// Sync 用于尽快将 Proxier 的内部状态同步到 iptables 与 IPVS。
func (proxier *Proxier) Sync() {
	if proxier.healthzServer != nil {
		proxier.healthzServer.QueuedUpdate(proxier.ipFamily)
	}
	// 记录排队时间，供同步延迟指标统计。
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Run()
}

// SyncLoop 执行周期性同步任务，通常作为 goroutine 或应用主循环运行，且不会返回。
func (proxier *Proxier) SyncLoop() {
	// 一开始就更新 healthz 时间戳，避免 Sync() 从未成功时健康检查误判。
	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}
	// informer 正在同步时没有真实的变更事件，这里人为写入一个“最后排队时间”。
	// 记录排队时间，供同步延迟指标统计。
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Loop(wait.NeverStop)
}

// setInitialized 以原子方式设置 Proxier 的初始化状态，避免与读取端产生数据竞争。
func (proxier *Proxier) setInitialized(value bool) {
	var initialized int32
	if value {
		initialized = 1
	}
	atomic.StoreInt32(&proxier.initialized, initialized)
}

// isInitialized 以原子方式读取 Proxier 是否已完成初始化。
func (proxier *Proxier) isInitialized() bool {
	return atomic.LoadInt32(&proxier.initialized) > 0
}

// OnServiceAdd 在观察到新建 Service 对象时被调用。
func (proxier *Proxier) OnServiceAdd(service *v1.Service) {
	proxier.OnServiceUpdate(nil, service)
}

// OnServiceUpdate 在观察到已有 Service 对象被修改时被调用。
func (proxier *Proxier) OnServiceUpdate(oldService, service *v1.Service) {
	if proxier.serviceChanges.Update(oldService, service) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnServiceDelete 在观察到已有 Service 对象被删除时被调用。
func (proxier *Proxier) OnServiceDelete(service *v1.Service) {
	proxier.OnServiceUpdate(service, nil)
}

// OnServiceSynced 在所有初始事件处理函数都已调用、状态完全同步到本地缓存后调用一次。
func (proxier *Proxier) OnServiceSynced() {
	proxier.mu.Lock()
	proxier.servicesSynced = true
	proxier.setInitialized(proxier.endpointSlicesSynced)
	proxier.mu.Unlock()

	// 这里无条件触发同步：该回调在整个生命周期中只调用一次。
	proxier.syncProxyRules()
}

// OnEndpointSliceAdd 在观察到新的 EndpointSlice 对象
// 时被调用。
func (proxier *Proxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSliceUpdate 在观察到已有 EndpointSlice 对象
// 被修改时被调用。
func (proxier *Proxier) OnEndpointSliceUpdate(_, endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSliceDelete 在观察到已有 EndpointSlice 对象
// 被删除时被调用。
func (proxier *Proxier) OnEndpointSliceDelete(endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, true) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSlicesSynced 在所有初始事件处理函数都已调用、
// 且状态完全同步到本地缓存后调用一次。
func (proxier *Proxier) OnEndpointSlicesSynced() {
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.setInitialized(proxier.servicesSynced)
	proxier.mu.Unlock()

	// 这里无条件触发同步：该回调在整个生命周期中只调用一次。
	proxier.syncProxyRules()
}

// OnTopologyChange 在本节点与代理相关的拓扑标签发生变化时被调用。
func (proxier *Proxier) OnTopologyChange(topologyLabels map[string]string) {
	proxier.mu.Lock()
	proxier.topologyLabels = topologyLabels
	proxier.mu.Unlock()
	proxier.logger.V(4).Info("Updated proxier node topology labels", "labels", topologyLabels)
	proxier.Sync()
}

// OnServiceCIDRsChanged 在 Service CIDR 发生变化时被调用，
// 参数提供完整的 Service CIDR 列表；IPVS 代理无需处理，故为空实现。
func (proxier *Proxier) OnServiceCIDRsChanged(_ []string) {}

// syncProxyRules 是实际下发 IPVS 与 iptables 规则的核心逻辑。
func (proxier *Proxier) syncProxyRules() (retryError error) {
	proxier.mu.Lock()
	defer proxier.mu.Unlock()

	// 在 Service 与 EndpointSlice 都完成初始同步前不下发规则，避免写入不完整数据。
	if !proxier.isInitialized() {
		proxier.logger.V(2).Info("Not syncing ipvs rules until Services and Endpoints have been received from master")
		return
	}

	// 此时可以安全地清除 initialSync：它只用于启动阶段，
	// 而且此处已经持有互斥锁。
	defer func() {
		proxier.initialSync = false
	}()

	// 记录本次同步耗时，用于指标与日志。
	start := time.Now()
	defer func() {
		metrics.SyncProxyRulesLatency.WithLabelValues(string(proxier.ipFamily)).Observe(metrics.SinceInSeconds(start))
		proxier.logger.V(4).Info("syncProxyRules complete", "elapsed", time.Since(start))
	}()

	// 调用本函数即表示确实需要同步，
	// 即使期间没有任何变更也是如此；换句话说，调用方需要
	// 自行判断是否为无效变更，避免不必要的调用。
	_ = proxier.svcPortMap.Update(proxier.serviceChanges)
	// 先把累积的变更应用到内存中的 Service/Endpoint 映射，后续规则全部基于新快照生成。
	endpointUpdateResult := proxier.endpointsMap.Update(proxier.endpointsChanges)

	proxier.logger.V(3).Info("Syncing ipvs proxier rules")

	proxier.serviceNoLocalEndpointsInternal = sets.New[string]()
	proxier.serviceNoLocalEndpointsExternal = sets.New[string]()

	proxier.lbNoNodeAccessIPPortProtocolEntries = make([]*utilipset.Entry, 0)

	// 开始准备 iptables 规则。

	// 复用缓冲区前先清空，
	// 避免反复分配内存，从而提升性能。
	proxier.natChains.Reset()
	proxier.natRules.Reset()
	proxier.filterChains.Reset()
	proxier.filterRules.Reset()

	// 写入 iptables-restore 所需的表头。
	proxier.filterChains.Write("*filter")
	proxier.natChains.Write("*nat")

	proxier.createAndLinkKubeChain()

	// 确保 dummy 网卡存在，IPVS 的 Service 地址需要绑定到它上面。
	// dummy 网卡是 VIP 的承载点，创建失败时后续规则无法生效，直接返回。
	_, err := proxier.netlinkHandle.EnsureDummyDevice(defaultDummyDevice)
	if err != nil {
		proxier.logger.Error(err, "Failed to create dummy interface", "interface", defaultDummyDevice)
		return
	}

	// 确保所需的 ipset 都已存在；每次同步都会重建其条目。
	for _, set := range proxier.ipsetList {
		// ipset 不存在时先创建，失败则中止本轮同步，避免生成引用不存在集合的规则。
		if err := ensureIPSet(set); err != nil {
			return
		}
		set.resetEntries()
	}

	// activeIPVSServices 记录本轮同步中成功创建的 IPVS Service。
	activeIPVSServices := sets.New[string]()
	// activeBindAddrs 记录本轮同步结束后应保留在 dummy 网卡上的地址。
	activeBindAddrs := sets.New[string]()
	// alreadyBoundAddrs 是 dummy 网卡当前已有的地址，用于判断是否需要重新绑定。
	alreadyBoundAddrs, err := proxier.netlinkHandle.GetLocalAddresses(defaultDummyDevice)
	if err != nil {
		proxier.logger.Error(err, "Error listing addresses binded to dummy interface")
	}
	// nodeAddressSet 是节点上除 dummy 网卡之外的所有地址。
	nodeAddressSet, err := proxier.netlinkHandle.GetAllLocalAddressesExcept(defaultDummyDevice)
	if err != nil {
		proxier.logger.Error(err, "Error listing node addresses")
	}

	// 先判断是否存在 NodePort 服务，只有存在时才需要计算节点 IP 列表。
	hasNodePort := false
	for _, svc := range proxier.svcPortMap {
		svcInfo, ok := svc.(*servicePortInfo)
		if ok && svcInfo.NodePort() != 0 {
			hasNodePort = true
			break
		}
	}

	// 存在 NodePort 服务时，同一批节点 IP 可复用于所有 NodePort Service，
	// 因此这里只计算一次。
	var nodeIPs []net.IP
	if hasNodePort {
		if proxier.nodePortAddresses.MatchAll() {
			for _, ipStr := range nodeAddressSet.UnsortedList() {
				nodeIPs = append(nodeIPs, netutils.ParseIPSloppy(ipStr))
			}
		} else {
			allNodeIPs, err := proxier.nodePortAddresses.GetNodeIPs(proxier.networkInterfacer)
			if err != nil {
				proxier.logger.Error(err, "Failed to get node IP address matching nodeport cidr")
			} else {
				for _, ip := range allNodeIPs {
					if !ip.IsLoopback() {
						nodeIPs = append(nodeIPs, ip)
					}
				}
			}
		}
	}

	// 逐个 Service 构建 IPVS 规则。
	for svcPortName, svcPort := range proxier.svcPortMap {
		svcInfo, ok := svcPort.(*servicePortInfo)
		if !ok {
			proxier.logger.Error(nil, "Failed to cast serviceInfo", "servicePortName", svcPortName)
			continue
		}

		protocol := strings.ToLower(string(svcInfo.Protocol()))
		// 预先计算名称字符串：Service 数量较多时，
		// 反复调用 ServicePortName.String() 会在 CPU profile 中显现。
		svcPortNameString := svcPortName.String()

		// 处理发端自身的回环流量（hairpin），通过 SNAT 改写源地址。
		for _, e := range proxier.endpointsMap[svcPortName] {
			ep, ok := e.(*proxy.BaseEndpointInfo)
			if !ok {
				proxier.logger.Error(nil, "Failed to cast BaseEndpointInfo", "endpoint", e)
				continue
			}
			if !ep.IsLocal() {
				continue
			}
			epIP := ep.IP()
			epPort := ep.Port()
			// 解析失败的 Endpoint 已记录日志，跳过即可。
			if epIP == "" || epPort == 0 {
				continue
			}
			entry := &utilipset.Entry{
				IP:       epIP,
				Port:     epPort,
				Protocol: protocol,
				IP2:      epIP,
				SetType:  utilipset.HashIPPortIP,
			}
			if valid := proxier.ipsetList[kubeLoopBackIPSet].validateEntry(entry); !valid {
				proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeLoopBackIPSet].Name)
				continue
			}
			proxier.ipsetList[kubeLoopBackIPSet].activeEntries.Insert(entry.String())
		}

		// 处理 ClusterIP，为其建立 IPVS 虚拟服务。
		// 调用 ipset 接口。
		entry := &utilipset.Entry{
			IP:       svcInfo.ClusterIP().String(),
			Port:     svcInfo.Port(),
			Protocol: protocol,
			SetType:  utilipset.HashIPPort,
		}
		// 将 ClusterIP:Port 加入 ipset，供 iptables 识别需要处理 hairpin 的流量。
		// proxier.kubeServiceAccessSet.activeEntries.Insert(entry.String())
		if valid := proxier.ipsetList[kubeClusterIPSet].validateEntry(entry); !valid {
			proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeClusterIPSet].Name)
			continue
		}
		proxier.ipsetList[kubeClusterIPSet].activeEntries.Insert(entry.String())
		// 调用 IPVS 接口。
		serv := &utilipvs.VirtualServer{
			Address:   svcInfo.ClusterIP(),
			Port:      uint16(svcInfo.Port()),
			Protocol:  string(svcInfo.Protocol()),
			Scheduler: proxier.ipvsScheduler,
		}
		// 为 IPVS Service 设置会话保持标志与超时时间。
		if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
			serv.Flags |= utilipvs.FlagPersistent
			serv.Timeout = uint32(svcInfo.StickyMaxAgeSeconds())
		}
		// "mh"（源地址哈希）调度算法需要额外设置 source hash 标志。
		if proxier.ipvsScheduler == "mh" {
			serv.Flags |= utilipvs.FlagSourceHash
		}
		// ClusterIP 必须绑定到 dummy 网卡，因此 syncService() 的 bindAddr 传 true。
		if err := proxier.syncService(svcPortNameString, serv, true, alreadyBoundAddrs); err == nil {
			activeIPVSServices.Insert(serv.String())
			activeBindAddrs.Insert(serv.Address.String())
			// ExternalTrafficPolicy 只作用于 NodePort 与外部 LB 流量，不影响 ClusterIP，
			// 因此即使 externalTrafficPolicy=Local，也仍需创建 ClusterIP 规则。
			internalNodeLocal := false
			if svcInfo.InternalPolicyLocal() {
				internalNodeLocal = true
			}
			if err := proxier.syncEndpoint(svcPortName, internalNodeLocal, serv); err != nil {
				proxier.logger.Error(err, "Failed to sync endpoint for service", "servicePortName", svcPortName, "virtualServer", serv)
			}
		} else {
			proxier.logger.Error(err, "Failed to sync service", "servicePortName", svcPortName, "virtualServer", serv)
		}

		// 处理 ExternalIP，为每个外部地址建立 IPVS 虚拟服务。
		for _, externalIP := range svcInfo.ExternalIPs() {
			// 调用 ipset 接口。
			entry := &utilipset.Entry{
				IP:       externalIP.String(),
				Port:     svcInfo.Port(),
				Protocol: protocol,
				SetType:  utilipset.HashIPPort,
			}

			if svcInfo.ExternalPolicyLocal() {
				if valid := proxier.ipsetList[kubeExternalIPLocalSet].validateEntry(entry); !valid {
					proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeExternalIPLocalSet].Name)
					continue
				}
				proxier.ipsetList[kubeExternalIPLocalSet].activeEntries.Insert(entry.String())
			} else {
				// 访问 ExternalIP 的数据包需要 SNAT。
				if valid := proxier.ipsetList[kubeExternalIPSet].validateEntry(entry); !valid {
					proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeExternalIPSet].Name)
					continue
				}
				proxier.ipsetList[kubeExternalIPSet].activeEntries.Insert(entry.String())
			}

			// 调用 IPVS 接口。
			serv := &utilipvs.VirtualServer{
				Address:   externalIP,
				Port:      uint16(svcInfo.Port()),
				Protocol:  string(svcInfo.Protocol()),
				Scheduler: proxier.ipvsScheduler,
			}
			if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
				serv.Flags |= utilipvs.FlagPersistent
				serv.Timeout = uint32(svcInfo.StickyMaxAgeSeconds())
			}
			// "mh"（源地址哈希）调度算法需要额外设置 source hash 标志。
			if proxier.ipvsScheduler == "mh" {
				serv.Flags |= utilipvs.FlagSourceHash
			}
			// 地址已存在于其他网卡时不能再绑定到 dummy 网卡，否则会造成冲突。
			shouldBind := !nodeAddressSet.Has(serv.Address.String())
			if err := proxier.syncService(svcPortNameString, serv, shouldBind, alreadyBoundAddrs); err == nil {
				activeIPVSServices.Insert(serv.String())
				if shouldBind {
					activeBindAddrs.Insert(serv.Address.String())
				}
				if err := proxier.syncEndpoint(svcPortName, svcInfo.ExternalPolicyLocal(), serv); err != nil {
					proxier.logger.Error(err, "Failed to sync endpoint for service", "servicePortName", svcPortName, "virtualServer", serv)
				}
			} else {
				proxier.logger.Error(err, "Failed to sync service", "servicePortName", svcPortName, "virtualServer", serv)
			}
		}

		// 处理 LoadBalancer 的 ingress IP，为其建立 IPVS 虚拟服务。
		for _, ingress := range svcInfo.LoadBalancerVIPs() {
			// 调用 ipset 接口。
			entry = &utilipset.Entry{
				IP:       ingress.String(),
				Port:     svcInfo.Port(),
				Protocol: protocol,
				SetType:  utilipset.HashIPPort,
			}
			// 将 LB 的 ingressIP:Port 加入 ipset，用于识别 hairpin 流量。
			// proxier.kubeServiceAccessSet.activeEntries.Insert(entry.String())
			// 全局代理时流量可能跨节点，因此需要 masquerade；
			// 仅本地代理时可以保留源 IP。
			if valid := proxier.ipsetList[kubeLoadBalancerSet].validateEntry(entry); !valid {
				proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeLoadBalancerSet].Name)
				continue
			}
			proxier.ipsetList[kubeLoadBalancerSet].activeEntries.Insert(entry.String())
			// externalTrafficPolicy=Local 时，还要把 LB 条目加入本地集合。
			if svcInfo.ExternalPolicyLocal() {
				if valid := proxier.ipsetList[kubeLoadBalancerLocalSet].validateEntry(entry); !valid {
					proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeLoadBalancerLocalSet].Name)
					continue
				}
				proxier.ipsetList[kubeLoadBalancerLocalSet].activeEntries.Insert(entry.String())
			}
			if len(svcInfo.LoadBalancerSourceRanges()) != 0 {
				// 服务防火墙规则依据 spec.loadBalancerSourceRanges 生成；
				// 目前只适用于保留源 IP 的负载均衡器，
				// 对把流量转发到 NodePort 的负载均衡器，这些规则不会生效。
				if valid := proxier.ipsetList[kubeLoadBalancerFWSet].validateEntry(entry); !valid {
					proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeLoadBalancerFWSet].Name)
					continue
				}
				proxier.ipsetList[kubeLoadBalancerFWSet].activeEntries.Insert(entry.String())
				allowFromNode := false
				for _, cidr := range svcInfo.LoadBalancerSourceRanges() {
					// 调用 ipset 接口。
					entry = &utilipset.Entry{
						IP:       ingress.String(),
						Port:     svcInfo.Port(),
						Protocol: protocol,
						Net:      cidr.String(),
						SetType:  utilipset.HashIPPortNet,
					}
					// 枚举所有允许访问的源 CIDR。
					if valid := proxier.ipsetList[kubeLoadBalancerSourceCIDRSet].validateEntry(entry); !valid {
						proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeLoadBalancerSourceCIDRSet].Name)
						continue
					}
					proxier.ipsetList[kubeLoadBalancerSourceCIDRSet].activeEntries.Insert(entry.String())

					if cidr.Contains(proxier.nodeIP) {
						allowFromNode = true
					}
				}
				// 通常会有 ip route 规则拦截后端主机访问 LB VIP 的请求，
				// 此时请求不经过负载均衡器，而是直接回环到本机。
				// 因此需要额外放行本机发出的这类请求。
				if allowFromNode {
					entry = &utilipset.Entry{
						IP:       ingress.String(),
						Port:     svcInfo.Port(),
						Protocol: protocol,
						IP2:      ingress.String(),
						SetType:  utilipset.HashIPPortIP,
					}
					// 枚举所有允许访问的源 IP。
					if valid := proxier.ipsetList[kubeLoadBalancerSourceIPSet].validateEntry(entry); !valid {
						proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", proxier.ipsetList[kubeLoadBalancerSourceIPSet].Name)
						continue
					}
					proxier.ipsetList[kubeLoadBalancerSourceIPSet].activeEntries.Insert(entry.String())
				} else {
					// nodeIP 不在任何 SourceRange 内，必须显式禁止节点访问该 LB IP。
					proxier.lbNoNodeAccessIPPortProtocolEntries = append(proxier.lbNoNodeAccessIPPortProtocolEntries, entry)

				}
			}
			// 调用 IPVS 接口。
			serv := &utilipvs.VirtualServer{
				Address:   ingress,
				Port:      uint16(svcInfo.Port()),
				Protocol:  string(svcInfo.Protocol()),
				Scheduler: proxier.ipvsScheduler,
			}
			if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
				serv.Flags |= utilipvs.FlagPersistent
				serv.Timeout = uint32(svcInfo.StickyMaxAgeSeconds())
			}
			// "mh"（源地址哈希）调度算法需要额外设置 source hash 标志。
			if proxier.ipvsScheduler == "mh" {
				serv.Flags |= utilipvs.FlagSourceHash
			}
			// 地址已存在于其他网卡时不能再绑定到 dummy 网卡，否则会造成冲突。
			shouldBind := !nodeAddressSet.Has(serv.Address.String())
			if err := proxier.syncService(svcPortNameString, serv, shouldBind, alreadyBoundAddrs); err == nil {
				activeIPVSServices.Insert(serv.String())
				if shouldBind {
					activeBindAddrs.Insert(serv.Address.String())
				}
				if err := proxier.syncEndpoint(svcPortName, svcInfo.ExternalPolicyLocal(), serv); err != nil {
					proxier.logger.Error(err, "Failed to sync endpoint for service", "servicePortName", svcPortName, "virtualServer", serv)
				}
			} else {
				proxier.logger.Error(err, "Failed to sync service", "servicePortName", svcPortName, "virtualServer", serv)
			}
		}

		if svcInfo.NodePort() != 0 {
			if len(nodeIPs) == 0 {
				// 计算节点地址失败时跳过 NodePort 配置，
				// 避免用错误的地址下发规则。
				continue
			}

			// NodePort 流量通常需要 SNAT，本地流量除外。
			// 调用 ipset 接口。

			var (
				nodePortSet *IPSet
				entries     []*utilipset.Entry
			)

			switch protocol {
			case utilipset.ProtocolTCP:
				nodePortSet = proxier.ipsetList[kubeNodePortSetTCP]
				entries = []*utilipset.Entry{{
					// bitmap 类型的 ipset 只需端口，无需提供 IP 信息。
					Port:     svcInfo.NodePort(),
					Protocol: protocol,
					SetType:  utilipset.BitmapPort,
				}}
			case utilipset.ProtocolUDP:
				nodePortSet = proxier.ipsetList[kubeNodePortSetUDP]
				entries = []*utilipset.Entry{{
					// bitmap 类型的 ipset 只需端口，无需提供 IP 信息。
					Port:     svcInfo.NodePort(),
					Protocol: protocol,
					SetType:  utilipset.BitmapPort,
				}}
			case utilipset.ProtocolSCTP:
				nodePortSet = proxier.ipsetList[kubeNodePortSetSCTP]
				// SCTP 使用 hash:ip,port 类型的 ipset，因此每个节点 IP 都要生成一条条目。
				entries = []*utilipset.Entry{}
				for _, nodeIP := range nodeIPs {
					entries = append(entries, &utilipset.Entry{
						IP:       nodeIP.String(),
						Port:     svcInfo.NodePort(),
						Protocol: protocol,
						SetType:  utilipset.HashIPPort,
					})
				}
			default:
				// 理论上不会走到这里（协议类型已经过校验）。
				proxier.logger.Error(nil, "Unsupported protocol type", "protocol", protocol)
			}
			if nodePortSet != nil {
				entryInvalidErr := false
				for _, entry := range entries {
					if valid := nodePortSet.validateEntry(entry); !valid {
						proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", nodePortSet.Name)
						entryInvalidErr = true
						break
					}
					nodePortSet.activeEntries.Insert(entry.String())
				}
				if entryInvalidErr {
					continue
				}
			}

			// 添加 externalTrafficPolicy=Local 对应的 NodePort 条目。
			if svcInfo.ExternalPolicyLocal() {
				var nodePortLocalSet *IPSet
				switch protocol {
				case utilipset.ProtocolTCP:
					nodePortLocalSet = proxier.ipsetList[kubeNodePortLocalSetTCP]
				case utilipset.ProtocolUDP:
					nodePortLocalSet = proxier.ipsetList[kubeNodePortLocalSetUDP]
				case utilipset.ProtocolSCTP:
					nodePortLocalSet = proxier.ipsetList[kubeNodePortLocalSetSCTP]
				default:
					// 理论上不会走到这里（协议类型已经过校验）。
					proxier.logger.Error(nil, "Unsupported protocol type", "protocol", protocol)
				}
				if nodePortLocalSet != nil {
					entryInvalidErr := false
					for _, entry := range entries {
						if valid := nodePortLocalSet.validateEntry(entry); !valid {
							proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", nodePortLocalSet.Name)
							entryInvalidErr = true
							break
						}
						nodePortLocalSet.activeEntries.Insert(entry.String())
					}
					if entryInvalidErr {
						continue
					}
				}
			}

			// 为每个节点 IP 建立 IPVS 内核虚拟服务。
			for _, nodeIP := range nodeIPs {
				// 调用 IPVS 接口。
				serv := &utilipvs.VirtualServer{
					Address:   nodeIP,
					Port:      uint16(svcInfo.NodePort()),
					Protocol:  string(svcInfo.Protocol()),
					Scheduler: proxier.ipvsScheduler,
				}
				if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
					serv.Flags |= utilipvs.FlagPersistent
					serv.Timeout = uint32(svcInfo.StickyMaxAgeSeconds())
				}
				// "mh"（源地址哈希）调度算法需要额外设置 source hash 标志。
				if proxier.ipvsScheduler == "mh" {
					serv.Flags |= utilipvs.FlagSourceHash
				}
				// 节点 IP 已存在于真实网卡上，无需绑定到 dummy 网卡，故 bindAddr 传 false。
				if err := proxier.syncService(svcPortNameString, serv, false, alreadyBoundAddrs); err == nil {
					activeIPVSServices.Insert(serv.String())
					if err := proxier.syncEndpoint(svcPortName, svcInfo.ExternalPolicyLocal(), serv); err != nil {
						proxier.logger.Error(err, "Failed to sync endpoint for service", "servicePortName", svcPortName, "virtualServer", serv)
					}
				} else {
					proxier.logger.Error(err, "Failed to sync service", "servicePortName", svcPortName, "virtualServer", serv)
				}
			}
		}

		if svcInfo.HealthCheckNodePort() != 0 {
			nodePortSet := proxier.ipsetList[kubeHealthCheckNodePortSet]
			entry := &utilipset.Entry{
				// bitmap 类型的 ipset 只需端口，无需提供 IP 信息。
				Port:     svcInfo.HealthCheckNodePort(),
				Protocol: "tcp",
				SetType:  utilipset.BitmapPort,
			}

			if valid := nodePortSet.validateEntry(entry); !valid {
				proxier.logger.Error(nil, "Error adding entry to ipset", "entry", entry, "ipset", nodePortSet.Name)
				continue
			}
			nodePortSet.activeEntries.Insert(entry.String())
		}
	}

	// KUBE-IPVS-IPS 集合保存本轮需要保留在 dummy 网卡上的地址。
	proxier.ipsetList[kubeIPVSSet].activeEntries = activeBindAddrs

	// 同步 ipset 条目（对内核中的实际集合做增量更新）。
	for _, set := range proxier.ipsetList {
		// 先同步 ipset，再生成引用这些集合的 iptables 规则，顺序不能颠倒。
		set.syncIPSetEntries()
	}

	// 根据 ipset 内容生成对应的 iptables 跳转规则，
	// 每个 ipset 只遍历一次，避免重复调用 iptables。
	proxier.writeIptablesRules()

	// 下发 iptables 规则。
	// 使用 NoFlushTables，避免清空表中非 Kubernetes 创建的链。
	proxier.iptablesData.Reset()
	proxier.iptablesData.Write(proxier.natChains.Bytes())
	proxier.iptablesData.Write(proxier.natRules.Bytes())
	proxier.iptablesData.Write(proxier.filterChains.Bytes())
	proxier.iptablesData.Write(proxier.filterRules.Bytes())

	proxier.logger.V(5).Info(
		"Restoring iptables", "natChains", proxier.natChains,
		"natRules", proxier.natRules, "filterChains", proxier.filterChains,
		"filterRules", proxier.filterRules)
	// 一次性原子替换规则；解析失败时打印出错位置附近的规则便于排查。
	err = proxier.iptables.RestoreAll(proxier.iptablesData.Bytes(), utiliptables.NoFlushTables, utiliptables.RestoreCounters)
	if err != nil {
		if pErr, ok := err.(utiliptables.ParseError); ok {
			lines := utiliptables.ExtractLines(proxier.iptablesData.Bytes(), pErr.Line(), 3)
			proxier.logger.Error(pErr, "Failed to execute iptables-restore", "rules", lines)
		} else {
			proxier.logger.Error(err, "Failed to execute iptables-restore", "rules", proxier.iptablesData.Bytes())
		}
		metrics.IPTablesRestoreFailuresTotal.WithLabelValues(string(proxier.ipFamily)).Inc()
		return
	}
	for name, lastChangeTriggerTimes := range endpointUpdateResult.LastChangeTriggerTimes {
		for _, lastChangeTriggerTime := range lastChangeTriggerTimes {
			latency := metrics.SinceInSeconds(lastChangeTriggerTime)
			metrics.NetworkProgrammingLatency.WithLabelValues(string(proxier.ipFamily)).Observe(latency)
			proxier.logger.V(4).Info("Network programming", "endpoint", klog.KRef(name.Namespace, name.Name), "elapsed", latency)
		}
	}

	// 清理 dummy 网卡上不再需要的地址。
	superfluousAddresses := alreadyBoundAddrs.Difference(activeBindAddrs)
	if superfluousAddresses.Len() > 0 {
		proxier.logger.V(2).Info("Removing addresses", "interface", defaultDummyDevice, "addresses", superfluousAddresses)
		for adr := range superfluousAddresses {
			if err := proxier.netlinkHandle.UnbindAddress(adr, defaultDummyDevice); err != nil {
				proxier.logger.Error(err, "UnbindAddress", "interface", defaultDummyDevice, "address", adr)
			}
		}
	}

	// currentIPVSServices 是从系统查询到的 IPVS Service，
	// 其中包含本轮刚创建的 Service。
	currentIPVSServices := make(map[string]*utilipvs.VirtualServer)
	appliedSvcs, err := proxier.ipvs.GetVirtualServers()
	if err == nil {
		for _, appliedSvc := range appliedSvcs {
			currentIPVSServices[appliedSvc.String()] = appliedSvc
		}
	} else {
		proxier.logger.Error(err, "Failed to get ipvs service")
	}
	// 对比期望集合与系统实际集合，删除本轮不再需要的旧 Service。
	proxier.cleanLegacyService(activeIPVSServices, currentIPVSServices)

	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}
	metrics.SyncProxyRulesLastTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()

	// 更新健康检查服务：Endpoint 列表可能包含非 OnlyLocal 的 Service，
	// 但 Service 列表只包含 OnlyLocal 的；serviceHealthServer
	// 会自动忽略多余的 Endpoint。
	if err := proxier.serviceHealthServer.SyncServices(proxier.svcPortMap.HealthCheckNodePorts()); err != nil {
		proxier.logger.Error(err, "Error syncing healthcheck services")
	}
	if err := proxier.serviceHealthServer.SyncEndpoints(proxier.endpointsMap.LocalReadyEndpoints()); err != nil {
		proxier.logger.Error(err, "Error syncing healthcheck endpoints")
	}

	metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("internal", string(proxier.ipFamily)).Set(float64(proxier.serviceNoLocalEndpointsInternal.Len()))
	metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("external", string(proxier.ipFamily)).Set(float64(proxier.serviceNoLocalEndpointsExternal.Len()))

	if endpointUpdateResult.ConntrackCleanupRequired {
		// 收尾工作：清理 UDP Service 的过期 conntrack 条目。
		conntrack.CleanStaleEntries(proxier.conntrack, proxier.ipFamily, proxier.svcPortMap, proxier.endpointsMap)
	}
	return
}

// writeIptablesRules 根据 ipsetList 与 ipsetWithIptablesChain 的对应关系，
// 将 IPVS Proxier 所需的 iptables 规则写入 natRules 或 filterRules。
// 部分 ipset（如 kubeClusterIPSet）有特殊的匹配与跳转规则，需要单独同步。
func (proxier *Proxier) writeIptablesRules() {

	// 在 Service 链的开头提前放行来自本地回环地址的连接。
	loAddr := "127.0.0.0/8"
	if proxier.ipFamily == v1.IPv6Protocol {
		loAddr = "::1/128"
	}
	proxier.natRules.Write("-A", string(kubeServicesChain), "-s", loAddr, "-j", "RETURN")

	// 在循环外一次性创建参数切片，避免每轮重新分配内存。
	// 复用内存时应采用下面的写法，而不是：
	//   slice = <some new slice>
	// 应当始终使用以下两种方式之一：
	//   slice = slice[:0] // 然后继续 append
	//   slice = append(slice[:0], ...)
	// 预分配 64 个槽位以避免扩容；
	// 单行规则的参数数量不会超过这个数值。
	// 即便超过 64 也仍然正确（append 会自动扩容），
	// 这里只是出于效率考虑，而非正确性要求。
	args := make([]string, 64)

	for _, set := range ipsetWithIptablesChain {
		if _, find := proxier.ipsetList[set.name]; find && !proxier.ipsetList[set.name].isEmpty() {
			args = append(args[:0], "-A", set.from)
			if set.protocolMatch != "" {
				args = append(args, "-p", set.protocolMatch)
			}
			args = append(args,
				"-m", "comment", "--comment", proxier.ipsetList[set.name].getComment(),
				"-m", "set", "--match-set", proxier.ipsetList[set.name].Name,
				set.matchType,
			)
			if set.table == utiliptables.TableFilter {
				proxier.filterRules.Write(args, "-j", set.to)
			} else {
				proxier.natRules.Write(args, "-j", set.to)
			}
		}
	}

	if !proxier.ipsetList[kubeClusterIPSet].isEmpty() {
		args = append(args[:0],
			"-A", string(kubeServicesChain),
			"-m", "comment", "--comment", proxier.ipsetList[kubeClusterIPSet].getComment(),
			"-m", "set", "--match-set", proxier.ipsetList[kubeClusterIPSet].Name,
		)
		if proxier.masqueradeAll {
			proxier.natRules.Write(
				args, "dst,dst",
				"-j", string(kubeMarkMasqChain))
		} else if proxier.localDetector.IsImplemented() {
			// 这会对集群外部访问 Service VIP 的流量做 masquerade。思路是：
			// 为 Service 网段配置一条静态路由，
			// 将流量导向任意节点，由该节点把流量转入 Service。
			// 由于流量可能被转发到其他节点，因此在这里做 masquerade。
			// 如果将来支持 VIP 的 "Local" 策略，需要同步调整这里。
			proxier.natRules.Write(
				args, "dst,dst",
				proxier.localDetector.IfNotLocal(),
				"-j", string(kubeMarkMasqChain))
		} else {
			// 对所有源地址为 Service IP 的 OUTPUT 流量做 masquerade。
			// dummy 网卡上绑定了所有 Service VIP，
			// 这会导致内核选择 Service VIP 作为访问另一 VIP 时的源 IP，
			// 从而形成 VIP:<随机端口> 到 VIP:<服务端口> 的连接。
			// （即 VIP 到自身的连接。）
			// 对节点主动发出、源 IP 为 VIP 的 OUTPUT 流量一律做 masquerade，
			// 可以修复这类出站连接。
			proxier.natRules.Write(
				args, "src,dst",
				"-j", string(kubeMarkMasqChain))
		}
	}

	// externalIPRules 写入适用于 Service ExternalIP 的 iptables 规则。
	externalIPRules := func(args []string) {
		// 放行来自机器外部（既不来自网桥，也不来自本地进程）访问 ExternalIP 的流量，
		// 使这些流量可以被转发到 Service。
		// 该规则大致等价于“所有来自本机之外的流量”。
		// 对不使用网桥的网络插件，这一判断并不精确，后续可以再完善。
		externalTrafficOnlyArgs := append(args,
			"-m", "physdev", "!", "--physdev-is-in",
			"-m", "addrtype", "!", "--src-type", "LOCAL")
		proxier.natRules.Write(externalTrafficOnlyArgs, "-j", "ACCEPT")
		dstLocalOnlyArgs := append(args, "-m", "addrtype", "--dst-type", "LOCAL")
		// 某些 ExternalIP（如 GCE 负载均衡器地址）会出现在本地路由表中，
		// 此时需要放行这些目标地址为本机地址的流量，使其保持本地转发。
		proxier.natRules.Write(dstLocalOnlyArgs, "-j", "ACCEPT")
	}

	if !proxier.ipsetList[kubeExternalIPSet].isEmpty() {
		// 为访问 ExternalIP 的数据包构建 masquerade 规则。
		args = append(args[:0],
			"-A", string(kubeServicesChain),
			"-m", "comment", "--comment", proxier.ipsetList[kubeExternalIPSet].getComment(),
			"-m", "set", "--match-set", proxier.ipsetList[kubeExternalIPSet].Name,
			"dst,dst",
		)
		proxier.natRules.Write(args, "-j", string(kubeMarkMasqChain))
		externalIPRules(args)
	}

	if !proxier.ipsetList[kubeExternalIPLocalSet].isEmpty() {
		args = append(args[:0],
			"-A", string(kubeServicesChain),
			"-m", "comment", "--comment", proxier.ipsetList[kubeExternalIPLocalSet].getComment(),
			"-m", "set", "--match-set", proxier.ipsetList[kubeExternalIPLocalSet].Name,
			"dst,dst",
		)
		externalIPRules(args)
	}

	// -A KUBE-SERVICES  -m addrtype  --dst-type LOCAL -j KUBE-NODE-PORT
	args = append(args[:0],
		"-A", string(kubeServicesChain),
		"-m", "addrtype", "--dst-type", "LOCAL",
	)
	proxier.natRules.Write(args, "-j", string(kubeNodePortChain))

	// 为 KUBE-LOAD-BALANCER 中的数据包打上 masquerade 标记。
	proxier.natRules.Write(
		"-A", string(kubeLoadBalancerChain),
		"-j", string(kubeMarkMasqChain),
	)

	// 丢弃被 KUBE-SOURCE-RANGES-FIREWALL 过滤的数据包。
	proxier.filterRules.Write(
		"-A", string(kubeSourceRangesFirewallChain),
		"-j", "DROP",
	)

	// 禁止从节点访问 LoadBalancer VIP。
	// IPVS 模式下源地址和目的地址都是 lbIP，因此需要按此特征匹配。
	for _, entry := range proxier.lbNoNodeAccessIPPortProtocolEntries {
		proxier.filterRules.Write(
			"-A", string(kubeIPVSOutFilterChain),
			"-s", entry.IP,
			"-m", "ipvs", "--vaddr", entry.IP, "--vproto", entry.Protocol, "--vport", strconv.Itoa(entry.Port),
			"-j", "DROP",
		)
	}

	// 放行所有目的地址为 IPVS 虚拟服务的数据包，避免被其他 iptables 规则
	// 拦截而导致 IPVS 规则失效。
	// 这些规则必须位于 KUBE-SERVICES 链的末尾。
	// ACCEPT 规则必须最后写入，以免前面的规则抢先丢弃 IPVS 流量。
	proxier.acceptIPVSTraffic()

	// 已打上 masqueradeMark 的流量同样予以放行，
	// 这样即使 FORWARD 链的默认策略不是 ACCEPT，NodePort 流量仍能被转发。
	// （该规则写入 FORWARD 链。）
	proxier.filterRules.Write(
		"-A", string(kubeForwardChain),
		"-m", "comment", "--comment", `"kubernetes forwarding rules"`,
		"-m", "mark", "--mark", fmt.Sprintf("%s/%s", proxier.masqueradeMark, proxier.masqueradeMark),
		"-j", "ACCEPT",
	)

	// 后续规则确保被上面 “kubernetes forwarding rules” 放行的连接，
	// 其后续报文（RELATED/ESTABLISHED）也能被接受。
	proxier.filterRules.Write(
		"-A", string(kubeForwardChain),
		"-m", "comment", "--comment", `"kubernetes forwarding conntrack rule"`,
		"-m", "conntrack",
		"--ctstate", "RELATED,ESTABLISHED",
		"-j", "ACCEPT",
	)

	// 添加放行健康检查 NodePort 流量的规则。
	proxier.filterRules.Write(
		"-A", string(kubeNodePortChain),
		"-m", "comment", "--comment", proxier.ipsetList[kubeHealthCheckNodePortSet].getComment(),
		"-m", "set", "--match-set", proxier.ipsetList[kubeHealthCheckNodePortSet].Name, "dst",
		"-j", "ACCEPT",
	)

	// 在 filter/KUBE-IPVS-FILTER 链中添加规则，防止通过 VIP 访问节点主机上的端口。
	// https://github.com/kubernetes/kubernetes/issues/72236
	proxier.filterRules.Write(
		"-A", string(kubeIPVSFilterChain),
		"-m", "set", "--match-set", proxier.ipsetList[kubeLoadBalancerSet].Name, "dst,dst", "-j", "RETURN")
	proxier.filterRules.Write(
		"-A", string(kubeIPVSFilterChain),
		"-m", "set", "--match-set", proxier.ipsetList[kubeClusterIPSet].Name, "dst,dst", "-j", "RETURN")
	proxier.filterRules.Write(
		"-A", string(kubeIPVSFilterChain),
		"-m", "set", "--match-set", proxier.ipsetList[kubeExternalIPSet].Name, "dst,dst", "-j", "RETURN")
	proxier.filterRules.Write(
		"-A", string(kubeIPVSFilterChain),
		"-m", "set", "--match-set", proxier.ipsetList[kubeExternalIPLocalSet].Name, "dst,dst", "-j", "RETURN")
	proxier.filterRules.Write(
		"-A", string(kubeIPVSFilterChain),
		"-m", "set", "--match-set", proxier.ipsetList[kubeHealthCheckNodePortSet].Name, "dst", "-j", "RETURN")
	proxier.filterRules.Write(
		"-A", string(kubeIPVSFilterChain),
		"-m", "conntrack", "--ctstate", "NEW",
		"-m", "set", "--match-set", proxier.ipsetList[kubeIPVSSet].Name, "dst", "-j", "REJECT")

	// 安装 Kubernetes 专用的 postrouting 规则；
	// 使用独立的链便于整体刷新和修改（例如标记值变化时）。
	// （例如标记值发生变化时。）

	proxier.natRules.Write(
		"-A", string(kubePostroutingChain),
		"-m", "mark", "!", "--mark", fmt.Sprintf("%s/%s", proxier.masqueradeMark, proxier.masqueradeMark),
		"-j", "RETURN",
	)
	// 清除标记，避免数据包再次经过网络栈时被重复 masquerade。
	proxier.natRules.Write(
		"-A", string(kubePostroutingChain),
		// 通过 XOR 操作清除 masqueradeMark 标记位。
		"-j", "MARK", "--xor-mark", proxier.masqueradeMark,
	)
	proxier.natRules.Write(
		"-A", string(kubePostroutingChain),
		"-m", "comment", "--comment", `"kubernetes service traffic requiring SNAT"`,
		"-j", "MASQUERADE",
		"--random-fully",
	)

	// 安装 Kubernetes 专用的 masquerade 标记规则；
	// 使用独立的链便于整体刷新和修改（例如标记值变化时）。
	// （例如标记值发生变化时。）
	proxier.natRules.Write(
		"-A", string(kubeMarkMasqChain),
		"-j", "MARK", "--or-mark", proxier.masqueradeMark,
	)

	// 写入表结束标记 COMMIT。
	proxier.filterRules.Write("COMMIT")
	proxier.natRules.Write("COMMIT")
}

// acceptIPVSTraffic 为 ClusterIP 与 LoadBalancer 的 ipset 写入 ACCEPT 规则，
// 防止其他 iptables 规则误拦截 IPVS 流量。
func (proxier *Proxier) acceptIPVSTraffic() {
	sets := []string{kubeClusterIPSet, kubeLoadBalancerSet}
	for _, set := range sets {
		var matchType string
		if !proxier.ipsetList[set].isEmpty() {
			switch proxier.ipsetList[set].SetType {
			case utilipset.BitmapPort:
				matchType = "dst"
			default:
				matchType = "dst,dst"
			}
			proxier.natRules.Write(
				"-A", string(kubeServicesChain),
				"-m", "set", "--match-set", proxier.ipsetList[set].Name, matchType,
				"-j", "ACCEPT",
			)
		}
	}
}

// createAndLinkKubeChain 创建 IPVS Proxier 所需的所有 kube 链，并建立基本的跳转关系。
func (proxier *Proxier) createAndLinkKubeChain() {
	for _, ch := range iptablesChains {
		if _, err := proxier.iptables.EnsureChain(ch.table, ch.chain); err != nil {
			proxier.logger.Error(err, "Failed to ensure chain exists", "table", ch.table, "chain", ch.chain)
			return
		}
		if ch.table == utiliptables.TableNAT {
			proxier.natChains.Write(utiliptables.MakeChainLine(ch.chain))
		} else {
			proxier.filterChains.Write(utiliptables.MakeChainLine(ch.chain))
		}
	}

	for _, jc := range iptablesJumpChain {
		args := []string{"-m", "comment", "--comment", jc.comment, "-j", string(jc.to)}
		if _, err := proxier.iptables.EnsureRule(utiliptables.Prepend, jc.table, jc.from, args...); err != nil {
			proxier.logger.Error(err, "Failed to ensure chain jumps", "table", jc.table, "srcChain", jc.from, "dstChain", jc.to)
		}
	}

}

// syncService 确保 IPVS 虚拟服务存在且与期望状态一致，并按需将服务地址绑定到 dummy 网卡。
func (proxier *Proxier) syncService(svcName string, vs *utilipvs.VirtualServer, bindAddr bool, alreadyBoundAddrs sets.Set[string]) error {
	appliedVirtualServer, _ := proxier.ipvs.GetVirtualServer(vs)
	if appliedVirtualServer == nil || !appliedVirtualServer.Equal(vs) {
		if appliedVirtualServer == nil {
			// 系统中不存在该 IPVS Service，需要新建。
			proxier.logger.V(3).Info("Adding new service", "serviceName", svcName, "virtualServer", vs)
			if err := proxier.ipvs.AddVirtualServer(vs); err != nil {
				proxier.logger.Error(err, "Failed to add IPVS service", "serviceName", svcName)
				return err
			}
		} else {
			// IPVS Service 配置发生变化，更新已有服务；
			// 更新过程中 VIP 不会中断服务。
			proxier.logger.V(3).Info("IPVS service was changed", "serviceName", svcName)
			if err := proxier.ipvs.UpdateVirtualServer(vs); err != nil {
				proxier.logger.Error(err, "Failed to update IPVS service")
				return err
			}
		}
	}

	// 将 Service 地址绑定到 dummy 网卡，使内核能够接收发往该 VIP 的流量。
	if bindAddr {
		// alreadyBoundAddrs 为 nil 时总是尝试绑定；
		// 否则先检查是否已绑定，已绑定则直接返回。
		if alreadyBoundAddrs != nil && alreadyBoundAddrs.Has(vs.Address.String()) {
			return nil
		}

		proxier.logger.V(4).Info("Bind address", "address", vs.Address)
		_, err := proxier.netlinkHandle.EnsureAddressBind(vs.Address.String(), defaultDummyDevice)
		if err != nil {
			proxier.logger.Error(err, "Failed to bind service address to dummy device", "serviceName", svcName)
			return err
		}
	}

	return nil
}

// syncEndpoint 使 IPVS 虚拟服务的 RealServer 列表与当前 Endpoint 集合保持一致，
// 并按流量策略与拓扑规则过滤 Endpoint。
func (proxier *Proxier) syncEndpoint(svcPortName proxy.ServicePortName, onlyNodeLocalEndpoints bool, vs *utilipvs.VirtualServer) error {
	appliedVirtualServer, err := proxier.ipvs.GetVirtualServer(vs)
	if err != nil {
		proxier.logger.Error(err, "Failed to get IPVS service")
		return err
	}
	if appliedVirtualServer == nil {
		return errors.New("IPVS virtual service does not exist")
	}

	// curEndpoints 是从系统查询到的当前 RealServer 集合。
	curEndpoints := sets.New[string]()
	curDests, err := proxier.ipvs.GetRealServers(appliedVirtualServer)
	if err != nil {
		proxier.logger.Error(err, "Failed to list IPVS destinations")
		return err
	}
	for _, des := range curDests {
		curEndpoints.Insert(des.String())
	}

	endpoints := proxier.endpointsMap[svcPortName]

	// 按拓扑感知规则过滤 Endpoint；只有在相关特性门控开启、
	// 且 Service 配置没有冲突时才会实际过滤，
	// 例如 externalTrafficPolicy=Local 就是冲突配置。
	// （此时直接按 Local 策略处理。）
	svcInfo, ok := proxier.svcPortMap[svcPortName]
	if !ok {
		proxier.logger.Info("Unable to filter endpoints due to missing service info", "servicePortName", svcPortName)
	} else {
		clusterEndpoints, localEndpoints, _, hasAnyEndpoints := proxy.CategorizeEndpoints(endpoints, svcInfo, proxier.nodeName, proxier.topologyLabels)
		if onlyNodeLocalEndpoints {
			if len(localEndpoints) > 0 {
				endpoints = localEndpoints
			} else {
				// https://github.com/kubernetes/kubernetes/pull/97081
				// 即使没有本地 Endpoint，也要允许本机 Pod 访问；
				// 否则外部流量仍会被转发，但回包的源地址是 Pod IP，
				// 最终会被丢弃。
				endpoints = clusterEndpoints

				if hasAnyEndpoints && svcInfo.InternalPolicyLocal() {
					proxier.serviceNoLocalEndpointsInternal.Insert(svcPortName.NamespacedName.String())
				}

				if hasAnyEndpoints && svcInfo.ExternalPolicyLocal() {
					proxier.serviceNoLocalEndpointsExternal.Insert(svcPortName.NamespacedName.String())
				}
			}
		} else {
			endpoints = clusterEndpoints
		}
	}

	newEndpoints := sets.New[string]()
	for _, epInfo := range endpoints {
		newEndpoints.Insert(epInfo.String())
	}

	// 添加新增的 Endpoint。
	for _, ep := range newEndpoints.UnsortedList() {
		ip, port, err := net.SplitHostPort(ep)
		if err != nil {
			proxier.logger.Error(err, "Failed to parse endpoint", "endpoint", ep)
			continue
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			proxier.logger.Error(err, "Failed to parse endpoint port", "port", port)
			continue
		}

		newDest := &utilipvs.RealServer{
			Address: netutils.ParseIPSloppy(ip),
			Port:    uint16(portNum),
			Weight:  1,
		}

		if curEndpoints.Has(ep) {
			// 首次同步时遍历已有 RealServer，
			// 将其权重重置为期望值。
			if proxier.initialSync {
				for _, dest := range curDests {
					if dest.Weight != newDest.Weight {
						err = proxier.ipvs.UpdateRealServer(appliedVirtualServer, newDest)
						if err != nil {
							proxier.logger.Error(err, "Failed to update destination", "newDest", newDest)
							continue
						}
					}
				}
			}
			// 若该 Endpoint 还在优雅删除列表中，需先移出再立即重建。
			uniqueRS := GetUniqueRSName(vs, newDest)
			if !proxier.gracefuldeleteManager.InTerminationList(uniqueRS) {
				continue
			}
			proxier.logger.V(5).Info("new ep is in graceful delete list", "uniqueRealServer", uniqueRS)
			err := proxier.gracefuldeleteManager.MoveRSOutofGracefulDeleteList(uniqueRS)
			if err != nil {
				proxier.logger.Error(err, "Failed to delete endpoint in gracefulDeleteQueue", "endpoint", ep)
				continue
			}
		}
		err = proxier.ipvs.AddRealServer(appliedVirtualServer, newDest)
		if err != nil {
			proxier.logger.Error(err, "Failed to add destination", "newDest", newDest)
			continue
		}
	}

	// 删除已消失的 Endpoint。
	for _, ep := range curEndpoints.Difference(newEndpoints).UnsortedList() {
		// 已进入优雅删除流程的 RealServer 不再重复处理。
		uniqueRS := vs.String() + "/" + ep
		if proxier.gracefuldeleteManager.InTerminationList(uniqueRS) {
			continue
		}
		ip, port, err := net.SplitHostPort(ep)
		if err != nil {
			proxier.logger.Error(err, "Failed to parse endpoint", "endpoint", ep)
			continue
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			proxier.logger.Error(err, "Failed to parse endpoint port", "port", port)
			continue
		}

		delDest := &utilipvs.RealServer{
			Address: netutils.ParseIPSloppy(ip),
			Port:    uint16(portNum),
		}

		proxier.logger.V(5).Info("Using graceful delete", "uniqueRealServer", uniqueRS)
		err = proxier.gracefuldeleteManager.GracefulDeleteRS(appliedVirtualServer, delDest)
		if err != nil {
			proxier.logger.Error(err, "Failed to delete destination", "uniqueRealServer", uniqueRS)
			continue
		}
	}
	return nil
}

// cleanLegacyService 删除本轮同步中不再需要的存量 IPVS Service。
func (proxier *Proxier) cleanLegacyService(activeServices sets.Set[string], currentServices map[string]*utilipvs.VirtualServer) {
	for cs, svc := range currentServices {
		if proxier.isIPInExcludeCIDRs(svc.Address) {
			continue
		}
		if getIPFamily(svc.Address) != proxier.ipFamily {
			// 属于其他 IP 协议族，由对应的 Proxier 负责清理。
			continue
		}
		if !activeServices.Has(cs) {
			proxier.logger.V(4).Info("Delete service", "virtualServer", svc)
			if err := proxier.ipvs.DeleteVirtualServer(svc); err != nil {
				proxier.logger.Error(err, "Failed to delete service", "virtualServer", svc)
			}
		}
	}
}

// isIPInExcludeCIDRs 判断 IP 是否落在用户配置的排除 CIDR 范围内。
func (proxier *Proxier) isIPInExcludeCIDRs(ip net.IP) bool {
	// 排除范围内的地址由用户自行管理，Proxier 不得清理。
	for _, excludedCIDR := range proxier.excludeCIDRs {
		if excludedCIDR.Contains(ip) {
			return true
		}
	}
	return false
}

// getIPFamily 返回给定 IP 所属的协议族（IPv4 或 IPv6）。
func getIPFamily(ip net.IP) v1.IPFamily {
	if netutils.IsIPv4(ip) {
		return v1.IPv4Protocol
	}
	return v1.IPv6Protocol
}

// IPVS Proxier 在需要对出口（egress）报文做 SNAT 时会回退到 iptables，
// 且只操作 iptables 的 *nat 表。
// 为需要 SNAT 的报文创建并链接 kube postrouting 链。
// Chain POSTROUTING（策略为 ACCEPT）
// target     prot opt source               destination
// KUBE-POSTROUTING  all  --  0.0.0.0/0            0.0.0.0/0            /* kubernetes postrouting rules *
// 由 kubelet 的网络同步循环维护。

// *nat
// :KUBE-POSTROUTING - [0:0]
// Chain KUBE-POSTROUTING（1 处引用）
// target     prot opt source               destination
// MASQUERADE  all  --  0.0.0.0/0            0.0.0.0/0            /* kubernetes service traffic requiring SNAT */ mark match 0x4000/0x4000

// :KUBE-MARK-MASQ - [0:0]
// Chain KUBE-MARK-MASQ（0 处引用）
// target     prot opt source               destination
// MARK       all  --  0.0.0.0/0            0.0.0.0/0            MARK or 0x4000
