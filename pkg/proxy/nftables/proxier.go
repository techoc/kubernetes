//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package nftables

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"net"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/conntrack"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	"k8s.io/kubernetes/pkg/proxy/localnodeportproxy"
	"k8s.io/kubernetes/pkg/proxy/metaproxier"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/kubernetes/pkg/proxy/runner"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/knftables"
)

const (
	// 我们的 nftables table。所有 chain/set/map 都建在该 table 内，
	// 因此它们的名字无需再加 "kube-" 或 "kube-proxy-" 前缀。
	kubeProxyTable = "kube-proxy"

	// base chain（直接挂载到 hook 的链）
	filterPreroutingPreDNATChain = "filter-prerouting-pre-dnat"
	filterOutputPreDNATChain     = "filter-output-pre-dnat"
	filterInputChain             = "filter-input"
	filterForwardChain           = "filter-forward"
	filterOutputChain            = "filter-output"
	natPreroutingChain           = "nat-prerouting"
	natOutputChain               = "nat-output"
	natPostroutingChain          = "nat-postrouting"

	// service 分发
	servicesChain       = "services"
	serviceIPsMap       = "service-ips"
	serviceNodePortsMap = "service-nodeports"

	// 允许接收 NodePort 流量的 IP 集合
	nodePortIPsSet = "nodeport-ips"

	// 活跃的 ClusterIP 集合。
	clusterIPsSet = "cluster-ips"

	// 无 Endpoint 的 service 处理
	serviceEndpointsCheckChain  = "service-endpoints-check"
	nodePortEndpointsCheckChain = "nodeport-endpoints-check"
	noEndpointServicesMap       = "no-endpoint-services"
	noEndpointNodePortsMap      = "no-endpoint-nodeports"
	rejectChain                 = "reject-chain"

	// 处理发往未分配 ClusterIP 以及 ClusterIP 未定义端口的流量
	clusterIPsCheckChain = "cluster-ips-check"

	// LoadBalancerSourceRanges 处理
	firewallIPsMap     = "firewall-ips"
	firewallCheckChain = "firewall-check"

	// 伪装（masquerade）
	masqueradingChain     = "masquerading"
	hairpinConnectionsSet = "hairpin-connections"

	// 对 kube-proxy 不提供服务的 localhost NodePort 进行 reject
	localhostNodePortRejectMap = "localhost-nodeport-reject-ports"
)

// localhostNodePortRejectChainName 返回按协议划分的 chain 名称，
// 该 chain 用于对 kube-proxy 不提供服务的 localhost NodePort 流量计数并拒绝。
func localhostNodePortRejectChainName(protocol v1.Protocol) string {
	return "localhost-nodeport-reject-" + strings.ToLower(string(protocol))
}

// NewDualStackProxier 创建一个包含 IPv4 和 IPv6 代理的 MetaProxier 实例。
func NewDualStackProxier(
	ctx context.Context,
	config *kubeproxyconfig.KubeProxyConfiguration,
	localDetectors map[v1.IPFamily]proxyutil.LocalTrafficDetector,
	nodeName string,
	nodeIPs map[v1.IPFamily]net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
	initOnly bool,
) (proxy.Provider, error) {
	// 先创建单栈 IPv4 proxier。
	ipv4Proxier, err := NewProxier(ctx, config, v1.IPv4Protocol,
		localDetectors[v1.IPv4Protocol], nodeName, nodeIPs[v1.IPv4Protocol],
		recorder, healthzServer, initOnly)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv4 proxier: %v", err)
	}

	ipv6Proxier, err := NewProxier(ctx, config, v1.IPv6Protocol,
		localDetectors[v1.IPv6Protocol], nodeName, nodeIPs[v1.IPv6Protocol],
		recorder, healthzServer, initOnly)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv6 proxier: %v", err)
	}
	// init-only 模式下只需完成环境初始化，无需构建 MetaProxier。
	if initOnly {
		return nil, nil
	}
	return metaproxier.NewMetaProxier(ipv4Proxier, ipv6Proxier), nil
}

// Proxier 是基于 nftables 的代理实现。
type Proxier struct {
	// ipFamily 表示该 proxier 所跟踪的 IP 协议族。
	ipFamily v1.IPFamily

	// endpointsChanges 和 serviceChanges 保存自上次 nftables 同步以来 Endpoint 和 Service 的所有变化。
	// 对单个对象，变化会被累积：previous 是所有这些变化之前的状态，
	// current 是应用所有这些变化之后的状态。
	endpointsChanges *proxy.EndpointsChangeTracker
	serviceChanges   *proxy.ServiceChangeTracker

	mu             sync.Mutex // 保护以下字段
	svcPortMap     proxy.ServicePortMap
	endpointsMap   proxy.EndpointsMap
	topologyLabels map[string]string
	// endpointSlicesSynced 和 servicesSynced 在启动后同步完相应对象时置为 true。
	// 用于避免 kube-proxy 重启后用不完整的数据更新 nftables。
	endpointSlicesSynced bool
	servicesSynced       bool
	lastFullSync         time.Time
	needFullSync         bool
	initialized          int32
	syncRunner           *runner.BoundedFrequencyRunner // 控制 syncProxyRules 的调用频率
	syncPeriod           time.Duration
	flushed              bool

	// 这些字段实际上等同于常量，无需持有互斥锁。
	nftables       knftables.Interface
	masqueradeAll  bool
	masqueradeMark string
	masqueradeRule string
	conntrack      conntrack.Interface
	localDetector  proxyutil.LocalTrafficDetector
	nodeName       string
	nodeIP         net.IP

	serviceHealthServer healthcheck.ServiceHealthServer
	healthzServer       *healthcheck.ProxyHealthServer

	// nodePortAddresses 选择 NodePort 生效的网卡。
	nodePortAddresses *proxyutil.NodePortAddresses
	// networkInterfacer 封装若干 net 库函数，
	// 测试时可注入替身。
	networkInterfacer proxyutil.NetworkInterfacer

	// staleChains 记录后续需要删除的 chain 及其时间。
	staleChains map[string]time.Time

	// serviceCIDRs 是本 proxier 所在 IP 协议族的 Service CIDR 列表（逗号分隔），
	// 可以直接被 knftables 使用。
	serviceCIDRs string

	logger         klog.Logger
	logRateLimiter *rate.Limiter

	clusterIPs          *nftElementStorage
	serviceIPs          *nftElementStorage
	firewallIPs         *nftElementStorage
	noEndpointServices  *nftElementStorage
	noEndpointNodePorts *nftElementStorage
	serviceNodePorts    *nftElementStorage
	hairpinConnections  *nftElementStorage

	// localhostNodePortRejects 保存 kube-proxy 选择拒绝而非提供服务的
	// localhost NodePort 的 (协议 . 端口) 键。
	localhostNodePortRejects *nftElementStorage

	// localhostNodePortProxy 负责 localhost 上 NodePort 的用户态代理。
	localhostNodePortProxy *localnodeportproxy.LocalNodePortProxy

	// localhostNodePortsEnabled 在 KubeProxyNFTablesLocalhostNodePorts 特性
	// 开启时置位。
	localhostNodePortsEnabled bool
}

// Proxier 实现 proxy.Provider 接口。
var _ proxy.Provider = &Proxier{}

// NewProxier 返回一个单栈 NFTables proxier。
func NewProxier(ctx context.Context,
	config *kubeproxyconfig.KubeProxyConfiguration,
	ipFamily v1.IPFamily,
	localDetector proxyutil.LocalTrafficDetector,
	nodeName string,
	nodeIP net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
	initOnly bool,
) (*Proxier, error) {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "ipFamily", ipFamily)

	nft, err := getNFTablesInterface(ipFamily)
	if err != nil {
		return nil, err
	}

	// init-only 模式只验证环境、不启动代理。
	if initOnly {
		logger.Info("System initialized and --init-only specified")
		return nil, nil
	}

	// 生成用于 SNAT 规则的 masquerade mark。
	masqueradeValue := 1 << uint(*config.NFTables.MasqueradeBit)
	masqueradeMark := fmt.Sprintf("%#08x", masqueradeValue)
	logger.V(2).Info("Using nftables mark for masquerade", "mark", masqueradeMark)

	nodePortAddresses := proxyutil.NewNodePortAddresses(ipFamily, config.NodePortAddresses)

	serviceHealthServer := healthcheck.NewServiceHealthServer(nodeName, recorder, nodePortAddresses, healthzServer, ipFamily)

	syncPeriod := config.SyncPeriod.Duration
	minSyncPeriod := config.MinSyncPeriod.Duration

	proxier := &Proxier{
		ipFamily:                 ipFamily,
		svcPortMap:               make(proxy.ServicePortMap),
		serviceChanges:           proxy.NewServiceChangeTracker(ipFamily, newServiceInfo, nil),
		endpointsMap:             make(proxy.EndpointsMap),
		endpointsChanges:         proxy.NewEndpointsChangeTracker(ipFamily, nodeName, newEndpointInfo, nil),
		needFullSync:             true,
		syncPeriod:               syncPeriod,
		nftables:                 nft,
		masqueradeAll:            config.Linux.MasqueradeAll,
		masqueradeMark:           masqueradeMark,
		masqueradeRule:           fmt.Sprintf("mark set mark or %s", masqueradeMark),
		conntrack:                conntrack.New(),
		localDetector:            localDetector,
		nodeName:                 nodeName,
		nodeIP:                   nodeIP,
		serviceHealthServer:      serviceHealthServer,
		healthzServer:            healthzServer,
		nodePortAddresses:        nodePortAddresses,
		networkInterfacer:        proxyutil.RealNetwork{},
		staleChains:              make(map[string]time.Time),
		logger:                   logger,
		logRateLimiter:           rate.NewLimiter(rate.Every(24*time.Hour), 1),
		clusterIPs:               newNFTElementStorage("set", clusterIPsSet),
		serviceIPs:               newNFTElementStorage("map", serviceIPsMap),
		firewallIPs:              newNFTElementStorage("map", firewallIPsMap),
		noEndpointServices:       newNFTElementStorage("map", noEndpointServicesMap),
		noEndpointNodePorts:      newNFTElementStorage("map", noEndpointNodePortsMap),
		serviceNodePorts:         newNFTElementStorage("map", serviceNodePortsMap),
		hairpinConnections:       newNFTElementStorage("set", hairpinConnectionsSet),
		localhostNodePortRejects: newNFTElementStorage("map", localhostNodePortRejectMap),
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeProxyNFTablesLocalhostNodePorts) {
		proxier.localhostNodePortsEnabled = true
		// 只有显式配置 loopback 地址时才需要用户态代理，其余情况由 reject 规则处理。
		if useLocalhostNodePortProxy(nodePortAddresses) {
			proxier.localhostNodePortProxy = localnodeportproxy.NewLocalNodePortProxy(ctx, ipFamily)
		}
	}

	logger.V(2).Info("NFTables sync params", "minSyncPeriod", minSyncPeriod, "syncPeriod", syncPeriod, "maxSyncPeriod", proxyutil.FullSyncPeriod)
	// 传给 NewBoundedFrequencyRunner 的 maxInterval 取任意值即可，这里用 time.Hour。
	proxier.syncRunner = runner.NewBoundedFrequencyRunner("sync-runner", proxier.syncProxyRules, minSyncPeriod, syncPeriod, proxyutil.FullSyncPeriod)

	return proxier, nil
}

// useLocalhostNodePortProxy 判断是否需要用户态代理 localhost 上的 NodePort。
func useLocalhostNodePortProxy(nodePortAddresses *proxyutil.NodePortAddresses) bool {
	return nodePortAddresses.ContainsExplicitLoopback()
}

// localhostNodePortRejectProtocols 返回 kube-proxy 会拒绝并计数的 localhost NodePort 协议列表；
// 若一个都不拒绝则返回 nil。启用用户态代理时，只拒绝代理无法服务的协议；
// 只开启特性但没有代理时，则全部拒绝。
func (proxier *Proxier) localhostNodePortRejectProtocols() []v1.Protocol {
	switch {
	case !proxier.localhostNodePortsEnabled:
		return nil
	case proxier.localhostNodePortProxy != nil:
		return metrics.LocalhostNodePortRejectedProtocols
	default:
		return metrics.LocalhostNodePortRejectableProtocols
	}
}

// rejectsLocalhostNodePort 报告对给定协议，kube-proxy 是否拒绝 localhost NodePort 流量。
func (proxier *Proxier) rejectsLocalhostNodePort(protocol v1.Protocol) bool {
	return slices.Contains(proxier.localhostNodePortRejectProtocols(), protocol)
}

// 保存 Service 信息的内部结构体，用于替代 serviceInfo。
type servicePortInfo struct {
	*proxy.BaseServicePortInfo
	// 以下字段预先算好并缓存，避免每次同步重复计算。
	nameString             string
	clusterPolicyChainName string
	localPolicyChainName   string
	externalChainName      string
	firewallChainName      string
}

// newServiceInfo 返回一个封装 serviceInfo 的 proxy.ServicePort。
func newServiceInfo(port *v1.ServicePort, service *v1.Service, bsvcPortInfo *proxy.BaseServicePortInfo) proxy.ServicePort {
	svcPort := &servicePortInfo{BaseServicePortInfo: bsvcPortInfo}

	// 预先算好并缓存以下字段，避免重复计算。
	svcName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	svcPortName := proxy.ServicePortName{NamespacedName: svcName, Port: port.Name}
	svcPort.nameString = svcPortName.String()

	chainNameBase := servicePortChainNameBase(&svcPortName, strings.ToLower(string(svcPort.Protocol())))
	svcPort.clusterPolicyChainName = servicePortPolicyClusterChainNamePrefix + chainNameBase
	svcPort.localPolicyChainName = servicePortPolicyLocalChainNamePrefix + chainNameBase
	svcPort.externalChainName = serviceExternalChainNamePrefix + chainNameBase
	svcPort.firewallChainName = servicePortFirewallChainNamePrefix + chainNameBase

	return svcPort
}

// 保存 Endpoint 信息的内部结构体。
type endpointInfo struct {
	*proxy.BaseEndpointInfo

	chainName       string
	affinitySetName string
}

// newEndpointInfo 返回一个封装 endpointInfo 的 proxy.Endpoint。
func newEndpointInfo(baseInfo *proxy.BaseEndpointInfo, svcPortName *proxy.ServicePortName) proxy.Endpoint {
	chainNameBase := servicePortEndpointChainNameBase(svcPortName, strings.ToLower(string(svcPortName.Protocol)), baseInfo.String())
	return &endpointInfo{
		BaseEndpointInfo: baseInfo,
		chainName:        servicePortEndpointChainNamePrefix + chainNameBase,
		affinitySetName:  servicePortEndpointAffinityNamePrefix + chainNameBase,
	}
}

// nftablesBaseChain 描述 "base chain"：直接挂载到 netfilter hook（如 postrouting、input
// 等）的链，区别于只有被规则 jump 到才会执行的 "普通" chain。参见
// https://wiki.nftables.org/wiki-nftables/index.php/Configuring_chains。
//
// 这些链由 setupNFTables() 创建，之后 syncProxyRules() 不会直接引用它们。
//
// 所有 base chain 的命名规则都是 "${type}-${hook}"，例如 "nat-prerouting"。
type nftablesBaseChain struct {
	name      string
	chainType knftables.BaseChainType
	hook      knftables.BaseChainHook
	priority  knftables.BaseChainPriority
}

// nftablesBaseChains 定义需要创建的 base chain 及其 hook/优先级。
var nftablesBaseChains = []nftablesBaseChain{
	// filter base chain（pre-dnat 优先级）：在 DNAT 前按原始目的 IP 和端口过滤 LoadBalancer source range。
	{filterPreroutingPreDNATChain, knftables.FilterType, knftables.PreroutingHook, knftables.DNATPriority + "-10"},
	{filterOutputPreDNATChain, knftables.FilterType, knftables.OutputHook, knftables.DNATPriority + "-10"},

	// filter base chain（filter 优先级）
	{filterInputChain, knftables.FilterType, knftables.InputHook, knftables.FilterPriority},
	{filterForwardChain, knftables.FilterType, knftables.ForwardHook, knftables.FilterPriority},
	{filterOutputChain, knftables.FilterType, knftables.OutputHook, knftables.FilterPriority},

	// nat base chain（dnat 优先级）
	{natPreroutingChain, knftables.NATType, knftables.PreroutingHook, knftables.DNATPriority},
	{natOutputChain, knftables.NATType, knftables.OutputHook, knftables.DNATPriority},
	{natPostroutingChain, knftables.NATType, knftables.PostroutingHook, knftables.SNATPriority},
}

// nftablesJumpChains 定义从 base chain 跳转到的顶层 chain。
type nftablesJumpChain struct {
	dstChain  string
	srcChain  string
	extraArgs string
}

// nftablesJumpChains 定义从 base chain 直接 jump 过来的顶层 "普通 chain"。
// 这些链由 setupNFTables() 创建，其中一部分也会被 syncProxyRules() 引用。
var nftablesJumpChains = []nftablesJumpChain{
	// 不能像 firewallCheckChain 那样从 filter-prerouting-pre-dnat 跳到 endpointsCheckChain：
	// 在 5.9 之前的内核上，reject 动作只在 input/forward/output hook 的链里合法。
	{nodePortEndpointsCheckChain, filterInputChain, "ct state new"},
	{serviceEndpointsCheckChain, filterInputChain, "ct state new"},
	{serviceEndpointsCheckChain, filterForwardChain, "ct state new"},
	{serviceEndpointsCheckChain, filterOutputChain, "ct state new"},

	{firewallCheckChain, filterPreroutingPreDNATChain, "ct state new"},
	{firewallCheckChain, filterOutputPreDNATChain, "ct state new"},

	{servicesChain, natOutputChain, ""},
	{servicesChain, natPreroutingChain, ""},
	{masqueradingChain, natPostroutingChain, ""},

	{clusterIPsCheckChain, filterForwardChain, "ct state new"},
	{clusterIPsCheckChain, filterOutputChain, "ct state new"},
}

// ensureChain 向 tx 添加命令，确保 chain 存在且不含本 transaction 之前的残留内容
// （用 createdChains 避免重复 Flush 同一条链，否则会把本次事务新加的规则也清掉）。
// skipCreation 为 true 时不会把 chain 加入事务，但仍会记入 createdChains，
// 以便在本轮同步结束时正确清理。
func ensureChain(chain string, tx *knftables.Transaction, createdChains sets.Set[string], skipCreation bool) {
	if createdChains.Has(chain) {
		return
	}
	createdChains.Insert(chain)
	if skipCreation {
		return
	}
	tx.Add(&knftables.Chain{
		Name: chain,
	})
	tx.Flush(&knftables.Chain{
		Name: chain,
	})
}

// setupNFTables 在事务中创建 kube-proxy table、base chain 和所有全局对象。
func (proxier *Proxier) setupNFTables(tx *knftables.Transaction) {
	ipX := "ip"
	ipvX_addr := "ipv4_addr" //nolint:staticcheck // var name intentionally resembles value
	matchLocalhost := "ip daddr 127.0.0.0/8"
	matchNotLocalhost := "ip daddr != 127.0.0.0/8"
	if proxier.ipFamily == v1.IPv6Protocol {
		ipX = "ip6"
		ipvX_addr = "ipv6_addr"
		matchLocalhost = "ip6 daddr ::1"
		matchNotLocalhost = "ip6 daddr != ::1"
	}

	tx.Add(&knftables.Table{
		Comment: ptr.To("rules for kube-proxy"),
	})

	// 额外做一次 add+delete，确保 table 里旧的 base chain 都会重建。
	// 否则修改这些 chain 的属性（如 priority）会导致事务失败。
	if !proxier.flushed {
		for _, bc := range nftablesBaseChains {
			chain := &knftables.Chain{
				Name: bc.name,
			}
			tx.Add(chain)
			tx.Delete(chain)
		}
		proxier.flushed = true
	}

	// 创建并清空 base chain
	for _, bc := range nftablesBaseChains {
		chain := &knftables.Chain{
			Name:     bc.name,
			Type:     ptr.To(bc.chainType),
			Hook:     ptr.To(bc.hook),
			Priority: ptr.To(bc.priority),
		}
		tx.Add(chain)
		tx.Flush(chain)
	}

	// 创建并清空普通 chain，并添加 jump 到这些 chain 的规则
	createdChains := sets.New[string]()
	for _, c := range nftablesJumpChains {
		ensureChain(c.dstChain, tx, createdChains, false)
		tx.Add(&knftables.Rule{
			Chain: c.srcChain,
			Rule: knftables.Concat(
				c.extraArgs,
				"jump", c.dstChain,
			),
		})
	}

	// 确保其它 "顶层" chain 都存在
	for _, chain := range []string{servicesChain, clusterIPsCheckChain, masqueradingChain} {
		ensureChain(chain, tx, createdChains, false)
	}

	// 添加 hairpin-connections 集合
	tx.Add(&knftables.Set{
		Name:    hairpinConnectionsSet,
		Type:    ipvX_addr + " . " + ipvX_addr + " . inet_proto . inet_service",
		Comment: ptr.To("service hairpin connections"),
	})

	// 在 masquerading chain 中添加规则
	tx.Add(&knftables.Rule{
		Chain: masqueradingChain,
		Rule: knftables.Concat(
			"mark", "and", proxier.masqueradeMark, "!=", "0",
			"mark", "set", "mark", "xor", proxier.masqueradeMark,
			"masquerade fully-random",
		),
	})
	tx.Add(&knftables.Rule{
		Chain: masqueradingChain,
		Rule: knftables.Concat(
			"ct status dnat",
			ipX, "saddr", ".", ipX, "daddr", ".", "meta l4proto", ".", "th dport", "@", hairpinConnectionsSet,
			"masquerade fully-random",
		),
	})

	// 添加 cluster-ips 集合。
	tx.Add(&knftables.Set{
		Name:    clusterIPsSet,
		Type:    ipvX_addr,
		Comment: ptr.To("Active ClusterIPs"),
	})

	// 拒绝发往 ClusterIP 无效端口的流量。
	tx.Add(&knftables.Rule{
		Chain: clusterIPsCheckChain,
		Rule: knftables.Concat(
			ipX, "daddr", "@", clusterIPsSet, "reject",
		),
		Comment: ptr.To("Reject traffic to invalid ports of ClusterIPs"),
	})

	// 丢弃发往未分配 ClusterIP 的流量。
	if len(proxier.serviceCIDRs) > 0 {
		tx.Add(&knftables.Rule{
			Chain: clusterIPsCheckChain,
			Rule: knftables.Concat(
				ipX, "daddr", "{", proxier.serviceCIDRs, "}",
				"drop",
			),
			Comment: ptr.To("Drop traffic to unallocated ClusterIPs"),
		})
	}

	// 按需填充 nodeport-ips 集合（不需要时删除它）。删除前先 add 再 delete，
	// 是因为集合本来就不存在时直接 delete 会报错。
	tx.Add(&knftables.Set{
		Name:    nodePortIPsSet,
		Type:    ipvX_addr,
		Comment: ptr.To("IPs that accept NodePort traffic"),
	})
	if proxier.nodePortAddresses.MatchAll() {
		tx.Delete(&knftables.Set{
			Name: nodePortIPsSet,
		})
	} else {
		tx.Flush(&knftables.Set{
			Name: nodePortIPsSet,
		})
		nodeIPs, err := proxier.nodePortAddresses.GetNodeIPs(proxier.networkInterfacer)
		if err != nil {
			proxier.logger.Error(err, "Failed to get node ip address matching nodeport cidrs, services with nodeport may not work as intended", "CIDRs", proxier.nodePortAddresses)
		}
		for _, ip := range nodeIPs {
			if ip.IsLoopback() {
				continue
			}
			tx.Add(&knftables.Element{
				Set: nodePortIPsSet,
				Key: []string{
					ip.String(),
				},
			})
		}
	}

	// 设置 "无 Endpoint" 时的 drop/reject 处理
	tx.Add(&knftables.Map{
		Name:    noEndpointServicesMap,
		Type:    ipvX_addr + " . inet_proto . inet_service : verdict",
		Comment: ptr.To("vmap to drop or reject packets to services with no endpoints"),
	})
	tx.Add(&knftables.Map{
		Name:    noEndpointNodePortsMap,
		Type:    "inet_proto . inet_service : verdict",
		Comment: ptr.To("vmap to drop or reject packets to service nodeports with no endpoints"),
	})

	tx.Add(&knftables.Chain{
		Name:    rejectChain,
		Comment: ptr.To("helper for @no-endpoint-services / @no-endpoint-nodeports"),
	})
	tx.Flush(&knftables.Chain{
		Name: rejectChain,
	})
	tx.Add(&knftables.Rule{
		Chain: rejectChain,
		Rule:  "reject",
	})

	tx.Add(&knftables.Rule{
		Chain: serviceEndpointsCheckChain,
		Rule: knftables.Concat(
			ipX, "daddr", ".", "meta l4proto", ".", "th dport",
			"vmap", "@", noEndpointServicesMap,
		),
	})

	if proxier.nodePortAddresses.MatchAll() {
		tx.Add(&knftables.Rule{
			Chain: nodePortEndpointsCheckChain,
			Rule: knftables.Concat(
				matchNotLocalhost,
				"meta l4proto . th dport",
				"vmap", "@", noEndpointNodePortsMap,
			),
		})
	} else {
		tx.Add(&knftables.Rule{
			Chain: nodePortEndpointsCheckChain,
			Rule: knftables.Concat(
				ipX, "daddr", "@", nodePortIPsSet,
				"meta l4proto . th dport",
				"vmap", "@", noEndpointNodePortsMap,
			),
		})
	}

	rejectProtocols := proxier.localhostNodePortRejectProtocols()
	rejected := make(map[v1.Protocol]bool, len(rejectProtocols))
	for _, protocol := range rejectProtocols {
		rejected[protocol] = true
	}
	tx.Add(&knftables.Map{
		Name:    localhostNodePortRejectMap,
		Type:    "inet_proto . inet_service : verdict",
		Comment: new("localhost NodePorts rejected by kube-proxy"),
	})
	if len(rejectProtocols) > 0 {
		for _, protocol := range rejectProtocols {
			counter := metrics.LocalhostNodePortRejectedCounterName(protocol)
			tx.Add(&knftables.Counter{Name: counter})
			chain := localhostNodePortRejectChainName(protocol)
			ensureChain(chain, tx, createdChains, false)
			tx.Add(&knftables.Rule{
				Chain: chain,
				Rule:  knftables.Concat("counter name", fmt.Sprintf("%q", counter), "reject"),
			})
		}
		tx.Add(&knftables.Rule{
			Chain: nodePortEndpointsCheckChain,
			Rule: knftables.Concat(
				matchLocalhost,
				"meta l4proto . th dport",
				"vmap", "@", localhostNodePortRejectMap,
			),
		})
	} else {
		tx.Delete(&knftables.Map{Name: localhostNodePortRejectMap})
	}
	// 清理当前不再拒绝的协议对应的 reject 对象（例如上一次同步配置不同留下的）。
	// 按依赖顺序删除：每条 chain 的规则依赖它的 counter。仍使用 add-then-delete，
	// 这样无论对象当前是否存在，delete 都能成功。
	for _, protocol := range metrics.LocalhostNodePortRejectableProtocols {
		if rejected[protocol] {
			continue
		}
		rejectChain := &knftables.Chain{Name: localhostNodePortRejectChainName(protocol)}
		tx.Add(rejectChain)
		tx.Delete(rejectChain)
		rejectCounter := &knftables.Counter{Name: metrics.LocalhostNodePortRejectedCounterName(protocol)}
		tx.Add(rejectCounter)
		tx.Delete(rejectCounter)
	}

	// 设置 LoadBalancerSourceRanges 防火墙
	tx.Add(&knftables.Map{
		Name:    firewallIPsMap,
		Type:    ipvX_addr + " . inet_proto . inet_service : verdict",
		Comment: ptr.To("destinations that are subject to LoadBalancerSourceRanges"),
	})

	ensureChain(firewallCheckChain, tx, createdChains, false)
	tx.Add(&knftables.Rule{
		Chain: firewallCheckChain,
		Rule: knftables.Concat(
			ipX, "daddr", ".", "meta l4proto", ".", "th dport",
			"vmap", "@", firewallIPsMap,
		),
	})

	// 设置 service 分发
	tx.Add(&knftables.Map{
		Name:    serviceIPsMap,
		Type:    ipvX_addr + " . inet_proto . inet_service : verdict",
		Comment: ptr.To("ClusterIP, ExternalIP and LoadBalancer IP traffic"),
	})
	tx.Add(&knftables.Map{
		Name:    serviceNodePortsMap,
		Type:    "inet_proto . inet_service : verdict",
		Comment: ptr.To("NodePort traffic"),
	})

	if proxier.masqueradeAll {
		tx.Add(&knftables.Rule{
			Chain: servicesChain,
			Rule: knftables.Concat(
				ipX, "daddr", "@", clusterIPsSet,
				proxier.masqueradeRule,
			),
			Comment: ptr.To("masquerade all clusterIP traffic"),
		})
	} else if proxier.localDetector.IsImplemented() {
		// 对集群外访问 Service VIP 的流量做 masquerade。
		// 思路是可以为 Service 网段配置一条静态路由，把流量引到任意节点，
		// 由该节点代为转发；由于流量可能再次跨节点，因此这里需要伪装源地址。
		tx.Add(&knftables.Rule{
			Chain: servicesChain,
			Rule: knftables.Concat(
				ipX, "daddr", "@", clusterIPsSet,
				proxier.localDetector.IfNotLocalNFT(),
				proxier.masqueradeRule,
			),
			Comment: ptr.To("masquerade clusterIP traffic from outside cluster"),
		})
	}

	tx.Add(&knftables.Rule{
		Chain: servicesChain,
		Rule: knftables.Concat(
			ipX, "daddr", ".", "meta l4proto", ".", "th dport",
			"vmap", "@", serviceIPsMap,
		),
	})
	if proxier.nodePortAddresses.MatchAll() {
		tx.Add(&knftables.Rule{
			Chain: servicesChain,
			Rule: knftables.Concat(
				"fib daddr type local",
				matchNotLocalhost,
				"meta l4proto . th dport",
				"vmap", "@", serviceNodePortsMap,
			),
		})
	} else {
		tx.Add(&knftables.Rule{
			Chain: servicesChain,
			Rule: knftables.Concat(
				ipX, "daddr @nodeport-ips",
				"meta l4proto . th dport",
				"vmap", "@", serviceNodePortsMap,
			),
		})
	}

	// 读回或清空集合：读回已有元素以支持增量更新，读失败则清空并重建。
	proxier.clusterIPs.readOrReset(tx, proxier.nftables, proxier.logger)
	proxier.serviceIPs.readOrReset(tx, proxier.nftables, proxier.logger)
	proxier.firewallIPs.readOrReset(tx, proxier.nftables, proxier.logger)
	proxier.noEndpointServices.readOrReset(tx, proxier.nftables, proxier.logger)
	proxier.noEndpointNodePorts.readOrReset(tx, proxier.nftables, proxier.logger)
	proxier.serviceNodePorts.readOrReset(tx, proxier.nftables, proxier.logger)
	proxier.hairpinConnections.readOrReset(tx, proxier.nftables, proxier.logger)
	if proxier.localhostNodePortsEnabled {
		proxier.localhostNodePortRejects.readOrReset(tx, proxier.nftables, proxier.logger)
	}
}

// Sync 用于尽快将 proxier 的状态同步到 nftables。
func (proxier *Proxier) Sync() {
	if proxier.healthzServer != nil {
		proxier.healthzServer.QueuedUpdate(proxier.ipFamily)
	}
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Run()
}

// SyncLoop 执行周期性任务。应作为 goroutine 或应用主循环运行，不会返回。
func (proxier *Proxier) SyncLoop() {
	// 同步开始时先更新 healthz 时间戳，防止 Sync() 一直未成功时状态失真。
	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}

	// informer 正在同步期间，合成一个 "最后一次变更入队" 的时间戳。
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Loop(wait.NeverStop)
}

// setInitialized 原子地设置 proxier 的初始化标志。
func (proxier *Proxier) setInitialized(value bool) {
	var initialized int32
	if value {
		initialized = 1
	}
	atomic.StoreInt32(&proxier.initialized, initialized)
}

// isInitialized 原子地读取 proxier 的初始化标志。
func (proxier *Proxier) isInitialized() bool {
	return atomic.LoadInt32(&proxier.initialized) > 0
}

// OnServiceAdd 在观察到新 Service 对象创建时调用。
func (proxier *Proxier) OnServiceAdd(service *v1.Service) {
	proxier.OnServiceUpdate(nil, service)
}

// OnServiceUpdate 在观察到已有 Service 对象被修改时调用。
func (proxier *Proxier) OnServiceUpdate(oldService, service *v1.Service) {
	if proxier.serviceChanges.Update(oldService, service) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnServiceDelete 在观察到已有 Service 对象被删除时调用。
func (proxier *Proxier) OnServiceDelete(service *v1.Service) {
	proxier.OnServiceUpdate(service, nil)
}

// OnServiceSynced 在所有初始事件回调完成、状态已完整写入本地缓存后调用。
func (proxier *Proxier) OnServiceSynced() {
	proxier.mu.Lock()
	proxier.servicesSynced = true
	proxier.setInitialized(proxier.endpointSlicesSynced)
	proxier.mu.Unlock()

	// 无条件同步：该方法每个生命周期只调用一次。
	proxier.syncProxyRules()
}

// OnEndpointSliceAdd 在观察到新 EndpointSlice 对象创建时调用。
func (proxier *Proxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSliceUpdate 在观察到已有 EndpointSlice 对象被修改时调用。
func (proxier *Proxier) OnEndpointSliceUpdate(_, endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSliceDelete 在观察到已有 EndpointSlice 对象被删除时调用。
func (proxier *Proxier) OnEndpointSliceDelete(endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, true) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSlicesSynced 在所有初始事件回调完成、状态已完整写入本地缓存后调用。
func (proxier *Proxier) OnEndpointSlicesSynced() {
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.setInitialized(proxier.servicesSynced)
	proxier.mu.Unlock()

	// 无条件同步：该方法每个生命周期只调用一次。
	proxier.syncProxyRules()
}

// OnTopologyChange 在本节点与代理相关的拓扑标签发生变化时调用。
func (proxier *Proxier) OnTopologyChange(topologyLabels map[string]string) {
	proxier.mu.Lock()
	proxier.topologyLabels = topologyLabels
	proxier.needFullSync = true
	proxier.mu.Unlock()
	proxier.logger.V(4).Info("Updated proxier node topology labels", "labels", topologyLabels)
	proxier.Sync()
}

// OnServiceCIDRsChanged 在 ServiceCIDR 发生变化时调用，参数为完整的 CIDR 列表。
func (proxier *Proxier) OnServiceCIDRsChanged(cidrs []string) {
	proxier.mu.Lock()
	defer proxier.mu.Unlock()

	cidrsForProxier := make([]string, 0)
	for _, cidr := range cidrs {
		isIPv4CIDR := netutils.IsIPv4CIDRString(cidr)
		if proxier.ipFamily == v1.IPv4Protocol && isIPv4CIDR {
			cidrsForProxier = append(cidrsForProxier, cidr)
		}

		if proxier.ipFamily == v1.IPv6Protocol && !isIPv4CIDR {
			cidrsForProxier = append(cidrsForProxier, cidr)
		}
	}
	proxier.serviceCIDRs = strings.Join(cidrsForProxier, ",")
}

const (
	// chain name 前缀（含末尾连字符）的最大长度。
	chainNamePrefixLengthMax = 16

	// servicePortChainNameBase 或 servicePortEndpointChainNameBase 返回的字符串的最大长度。
	chainNameBaseLengthMax = knftables.NameLengthMax - chainNamePrefixLengthMax
)

// 各类服务/Endpoint chain 与 affinity 集合的名称前缀。
const (
	servicePortPolicyClusterChainNamePrefix = "service-"
	servicePortPolicyLocalChainNamePrefix   = "local-"
	serviceExternalChainNamePrefix          = "external-"
	servicePortEndpointChainNamePrefix      = "endpoint-"
	servicePortEndpointAffinityNamePrefix   = "affinity-"
	servicePortFirewallChainNamePrefix      = "firewall-"
)

// hashAndTruncate 在 name 前加哈希前缀后截断到 chainNameBaseLengthMax。
// 哈希保证：(a) 截断尾部后名字仍唯一；(b) 与仅差一位数字的 endpoint chain 等
// 名字相近的链在视觉上也能区分开。
func hashAndTruncate(name string) string {
	hash := sha256.Sum256([]byte(name))
	encoded := base32.StdEncoding.EncodeToString(hash[:])
	name = encoded[:8] + "-" + name
	if len(name) > chainNameBaseLengthMax {
		name = name[:chainNameBaseLengthMax-3] + "..."
	}
	return name
}

// truncateNFTablesComment 将 s 截断到 nftables 接受的最大字节长度。
// 注释只用于展示，截断不会影响转发行为。
func truncateNFTablesComment(s string) string {
	if len(s) > knftables.CommentLengthMax {
		return s[:knftables.CommentLengthMax]
	}
	return s
}

// servicePortChainNameBase 返回给定 ServicePort 对应 chain 的基础名，
// 形如 "HASH-namespace/serviceName/protocol/portName"，例如
// "ULMVA6XW-ns1/svc1/tcp/p80"。
func servicePortChainNameBase(servicePortName *proxy.ServicePortName, protocol string) string {
	// nftables chain 名可包含 [A-Za-z0-9_./-]（但首字符必须是字母、下划线或点）。
	//
	// Namespace、Service 和 Port 名可包含 [a-z0-9-]（还有其它限制，但这里无关）。
	//
	// Protocol 取值为 /(tcp|udp|sctp)/。
	//
	// 因此可以原样把这些字段拼进 chain 名（前提是名字不会以这些字段开头，否则可能非法）。
	// 用 "/" 分隔各部分：这是 chain 名允许、而输入字段不允许的两个字符之一。

	name := fmt.Sprintf("%s/%s/%s/%s",
		servicePortName.NamespacedName.Namespace,
		servicePortName.NamespacedName.Name,
		protocol,
		servicePortName.Port,
	)

	// Namespace、Service、Port 名最长各 63 字符，Protocol 最长 4，
	// 加上 8 字符哈希和 4 个标点，总计 205，小于 chainNameBaseLengthMax (240)。
	// 因此这里实际上永远不会触发截断。
	return hashAndTruncate(name)
}

// servicePortEndpointChainNameBase 返回给定 Endpoint 的 chain 名后缀，形如
// "HASH-namespace/serviceName/protocol/portName__endpointIP/endpointport"，例如
// "5OJB2KTY-ns1/svc1/tcp/p80__10.180.0.1/80"。
func servicePortEndpointChainNameBase(servicePortName *proxy.ServicePortName, protocol, endpoint string) string {
	// 与 servicePortChainNameBase 相同：Namespace、Service、Port、Protocol 和
	// EndpointPort 都可原样拼进 chain 名。但 IPv6 EndpointIP 含冒号，chain 名不允许；
	// IPv6 地址也很长，但不能安全地截断（例如只留最后一段），因为（尤其是手工创建的
	// external endpoint）无法确定哪一部分是冗余的。

	endpointIP, endpointPort, _ := net.SplitHostPort(endpoint)
	if strings.Contains(endpointIP, ":") {
		endpointIP = strings.ReplaceAll(endpointIP, ":", ".")
	}

	// 同样用 "/" 分隔各部分，用 "__" 分隔 service 部分和 endpoint 部分。
	name := fmt.Sprintf("%s/%s/%s/%s__%s/%s",
		servicePortName.NamespacedName.Namespace,
		servicePortName.NamespacedName.Name,
		protocol,
		servicePortName.Port,
		endpointIP,
		endpointPort,
	)

	// "__" 之前的部分最长 205 字符（同 servicePortChainNameBase）。IPv6 地址最长
	// 39 字符，端口最长 5 位，再加 3 个标点，总长 252，超过 chainNameBaseLengthMax (240)，
	// 因此理论上可能触发截断（尽管极其罕见）。
	return hashAndTruncate(name)
}

// isServiceChainName 通过 chain 名中是否含 "/" 判断是否为服务相关的 chain。
func isServiceChainName(chainString string) bool {
	// servicePortChainNameBase 和 servicePortEndpointChainNameBase 返回的 chain 名
	// 一定至少包含一个 "/"，而内置 chain 名都不含斜杠，据此即可区分。
	return strings.Contains(chainString, "/")
}

// isAffinitySetName 判断集合是否为 endpoint affinity 集合。
func isAffinitySetName(set string) bool {
	return strings.HasPrefix(set, servicePortEndpointAffinityNamePrefix)
}

// nftElementStorage 是 nftables map 或 set 的内部表示。
type nftElementStorage struct {
	elements      map[string]string
	leftoverKeys  sets.Set[string]
	containerType string
	containerName string
}

// joinNFTSlice 把 nft 元素 key/value（[]string）拼接成字符串存入 nftElementStorage。
// 分隔符与 nft 命令一致，因此解析不会有歧义。
func joinNFTSlice(k []string) string {
	return strings.Join(k, " . ")
}

// splitNFTSlice 把 nftElementStorage 中的 key/value 字符串还原成切片。
func splitNFTSlice(k string) []string {
	return strings.Split(k, " . ")
}

// newNFTElementStorage 创建一个空的 nftElementStorage。
// 首次使用前必须调用 nftElementStorage.reset()。
func newNFTElementStorage(containerType, containerName string) *nftElementStorage {
	c := &nftElementStorage{
		elements:      make(map[string]string),
		leftoverKeys:  sets.New[string](),
		containerType: containerType,
		containerName: containerName,
	}
	return c
}

// readOrReset 从 nftables map/set 读回现有元素。
// 读取失败时清空内部状态并 flush nftables 中的 map/set，随后重建。
func (s *nftElementStorage) readOrReset(tx *knftables.Transaction, nftables knftables.Interface, logger klog.Logger) {
	clear(s.elements)
	// 无论读取成功与否都重置残留键集合，保证调用方看到的状态一致。
	defer s.resetLeftoverKeys()
	elems, err := nftables.ListElements(context.TODO(), s.containerType, s.containerName)
	// 集合不存在说明没有旧元素，属于正常情况，不需要走失败分支。
	if err != nil && !knftables.IsNotFound(err) {
		if s.containerType == "set" {
			tx.Flush(&knftables.Set{
				Name: s.containerName,
			})
		} else {
			tx.Flush(&knftables.Map{
				Name: s.containerName,
			})
		}
		logger.Error(err, "Failed to list nftables elements", "containerName", s.containerName, "containerType", s.containerType)
		return
	}
	for _, elem := range elems {
		newKey := joinNFTSlice(elem.Key)
		newValue := joinNFTSlice(elem.Value)
		s.elements[newKey] = newValue
	}
}

// resetLeftoverKeys 只能由 nftElementStorage 内部方法调用。
func (s *nftElementStorage) resetLeftoverKeys() {
	clear(s.leftoverKeys)
	for key := range s.elements {
		s.leftoverKeys.Insert(key)
	}
}

// ensureElem 若元素不在容器中则加入事务，并更新内部 leftoverKeys 以跟踪未使用的元素。
func (s *nftElementStorage) ensureElem(tx *knftables.Transaction, elem *knftables.Element) {
	newKey := joinNFTSlice(elem.Key)
	newValue := joinNFTSlice(elem.Value)
	existingValue, exists := s.elements[newKey]
	if exists {
		if existingValue != newValue {
			// 值不同则先删再加：nftables 不支持原地修改元素值。
			tx.Delete(elem)
			tx.Add(elem)
			s.elements[newKey] = newValue
		}
		delete(s.leftoverKeys, newKey)
	} else {
		tx.Add(elem)
		s.elements[newKey] = newValue
	}
}

// cleanupLeftoverKeys 删除上一轮残留、本轮未使用的元素。
func (s *nftElementStorage) cleanupLeftoverKeys(tx *knftables.Transaction) {
	for key := range s.leftoverKeys {
		e := &knftables.Element{
			Key: splitNFTSlice(key),
		}
		if s.containerType == "set" {
			e.Set = s.containerName
		} else {
			e.Map = s.containerName
		}
		tx.Delete(e)
		delete(s.elements, key)
	}
	s.resetLeftoverKeys()
}

// logFailure 以受限频率打印失败的事务及整张 table。
func (proxier *Proxier) logFailure(tx *knftables.Transaction) {
	if klogV4 := klog.V(4); klogV4.Enabled() && proxier.logRateLimiter.Allow() {
		klogV4.InfoS("Failed transaction", "transaction", tx.String())
		// knftables 暂不支持列出整表，这里用 nft 命令绕过。
		cmd := exec.Command("nft", "list", "table", kubeProxyTable)
		out, err := cmd.Output()
		if err != nil {
			klogV4.InfoS("Listing full table failed", "error", err)
		} else {
			klogV4.InfoS("Listing full table", "result", string(out))
		}
	}
}

// syncProxyRules 是所有 nftables 调用的入口。
// 调用时假定未持有 proxier.mu（函数内部自行加锁）。
func (proxier *Proxier) syncProxyRules() (retryError error) {
	proxier.mu.Lock()
	defer proxier.mu.Unlock()

	// 收到 Service 和 Endpoint 之前不写规则。
	if !proxier.isInitialized() {
		proxier.logger.V(2).Info("Not syncing nftables until Services and Endpoints have been received from master")
		return
	}

	// 记录每次同步耗时。
	start := time.Now()

	// 从此处开始，直到尝试写入 nftables 规则之前都不会提前返回。

	// 显式请求或距上次全量同步超时时，执行全量同步。
	doFullSync := proxier.needFullSync || (time.Since(proxier.lastFullSync) > proxyutil.FullSyncPeriod)

	defer func() {
		metrics.SyncProxyRulesLatency.WithLabelValues(string(proxier.ipFamily)).Observe(metrics.SinceInSeconds(start))
		if !doFullSync {
			metrics.SyncPartialProxyRulesLatency.WithLabelValues(string(proxier.ipFamily)).Observe(metrics.SinceInSeconds(start))
		} else {
			metrics.SyncFullProxyRulesLatency.WithLabelValues(string(proxier.ipFamily)).Observe(metrics.SinceInSeconds(start))
		}
		proxier.logger.V(2).Info("SyncProxyRules complete", "elapsed", time.Since(start))
	}()

	serviceUpdateResult := proxier.svcPortMap.Update(proxier.serviceChanges)
	endpointUpdateResult := proxier.endpointsMap.Update(proxier.endpointsChanges)

	proxier.logger.V(2).Info("Syncing nftables rules", "fullSync", doFullSync)

	success := false
	defer func() {
		if !success {
			proxier.logger.Info("Sync failed", "retryingTime", proxier.syncPeriod)
			retryError = fmt.Errorf("Sync failed")
			// serviceChanges 和 endpointChanges 已经被清空，无法再做增量同步，
			// 因此失败后下一轮必须走全量同步。
			proxier.needFullSync = true
		} else if doFullSync {
			proxier.lastFullSync = time.Now()
		}
	}()

	// 若上一轮事务留下足够旧的 chain，趁现在尝试删除。
	if len(proxier.staleChains) > 0 {
		oneSecondAgo := start.Add(-time.Second)
		tx := proxier.nftables.NewTransaction()
		deleted := 0
		for chain, modtime := range proxier.staleChains {
			if modtime.Before(oneSecondAgo) {
				tx.Delete(&knftables.Chain{
					Name: chain,
				})
				delete(proxier.staleChains, chain)
				deleted++
			}
		}
		if deleted > 0 {
			proxier.logger.Info("Deleting stale nftables chains", "numChains", deleted)
			err := proxier.nftables.Run(context.TODO(), tx)
			if err != nil {
				// 条目已经从 staleChains 移除，但如果 chain 实际还在，
				// 本轮结束时会带上更新的时间戳重新加入。
				proxier.logger.Error(err, "Unable to delete stale chains; will retry later")
				metrics.NFTablesCleanupFailuresTotal.WithLabelValues(string(proxier.ipFamily)).Inc()
				doFullSync = true

				// 打印失败的事务并列出完整的 kube-proxy table。
				proxier.logFailure(tx)
			}
		}
	}

	// 开始本轮真正的事务。
	tx := proxier.nftables.NewTransaction()
	if doFullSync {
		proxier.setupNFTables(tx)
	}

	// IPv4 用 "ip daddr"，IPv6 用 "ip6 daddr"。
	ipX := "ip"
	ipvX_addr := "ipv4_addr" //nolint:staticcheck // var name intentionally resembles value
	if proxier.ipFamily == v1.IPv6Protocol {
		ipX = "ip6"
		ipvX_addr = "ipv6_addr"
	}

	var existingChains sets.Set[string]
	existingChainsList, err := proxier.nftables.List(context.TODO(), "chain")
	// 列取失败只记录日志，按空集合继续，避免中断本轮同步。
	if err == nil {
		existingChains = sets.New(existingChainsList...)
	} else {
		proxier.logger.Error(err, "Failed to list existing chains")
	}
	var existingAffinitySets sets.Set[string]
	existingSets, err := proxier.nftables.List(context.TODO(), "sets")
	// affinity set 列表失败同样降级为空集合处理。
	if err == nil {
		existingAffinitySets = sets.New[string]()
		for _, set := range existingSets {
			if isAffinitySetName(set) {
				existingAffinitySets.Insert(set)
			}
		}
	} else {
		proxier.logger.Error(err, "Failed to list existing sets")
	}

	// 收集需要保留的 service/endpoint chain 和 affinity set。
	activeChains := sets.New[string]()
	activeAffinitySets := sets.New[string]()

	// 统计所有 service 的 endpoint 总数，用于估算集群规模。
	totalEndpoints := 0
	for svcName := range proxier.svcPortMap {
		totalEndpoints += len(proxier.endpointsMap[svcName])
	}

	// 这两个变量用于上报 sync_proxy_rules_no_endpoints_total 指标。
	serviceNoLocalEndpointsTotalInternal := 0
	serviceNoLocalEndpointsTotalExternal := 0

	var localhostNodePorts []localnodeportproxy.NodePortSpec

	// 逐个 service-port 生成规则。
	for svcName, svc := range proxier.svcPortMap {
		svcInfo, ok := svc.(*servicePortInfo)
		if !ok {
			proxier.logger.Error(nil, "Failed to cast serviceInfo", "serviceName", svcName)
			continue
		}

		protocol := strings.ToLower(string(svcInfo.Protocol()))
		svcPortComment := truncateNFTablesComment(svcInfo.nameString)

		// 分别计算 Cluster 和 Local 流量策略可用的 Endpoint。
		// allLocallyReachableEndpoints 是在给定服务流量策略下，从本节点可达的
		// 所有 Endpoint；hasEndpoints 表示任意节点上是否存在可用 Endpoint。
		allEndpoints := proxier.endpointsMap[svcName]
		clusterEndpoints, localEndpoints, allLocallyReachableEndpoints, hasEndpoints := proxy.CategorizeEndpoints(allEndpoints, svcInfo, proxier.nodeName, proxier.topologyLabels)

		// skipServiceUpdate 控制该服务所有 chain 及元素是否可以跳过：
		// 服务和 Endpoint 都没变化时无需重写。
		skipServiceUpdate := !doFullSync &&
			!serviceUpdateResult.UpdatedServices.Has(svcName.NamespacedName) &&
			!endpointUpdateResult.UpdatedServices.Has(svcName.NamespacedName)

		// 只有配置会话亲和性时才使用独立的 endpoint chain。
		serviceUsesAffinity := svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP

		// 标记本轮实际会用到的 Endpoint。
		for _, ep := range allLocallyReachableEndpoints {
			epInfo, ok := ep.(*endpointInfo)
			if !ok {
				continue
			}

			// 使用亲和性时，记下 endpoint chain 和 affinity set 的名字，
			// 并确保 chain 存在。
			if serviceUsesAffinity {
				ensureChain(epInfo.chainName, tx, activeChains, skipServiceUpdate)
				activeAffinitySets.Insert(epInfo.affinitySetName)
			}

			// 本地 Endpoint 加入 hairpin 集合。
			if epInfo.IsLocal() {
				proxier.hairpinConnections.ensureElem(tx, &knftables.Element{
					Set: hairpinConnectionsSet,
					Key: []string{
						epInfo.IP(),
						epInfo.IP(),
						protocol,
						strconv.Itoa(epInfo.Port()),
					},
				})
			}
		}

		// clusterPolicyChain 存放 "Cluster" 策略使用的 Endpoint。
		clusterPolicyChain := svcInfo.clusterPolicyChainName
		usesClusterPolicyChain := len(clusterEndpoints) > 0 && svcInfo.UsesClusterEndpoints()
		if usesClusterPolicyChain {
			ensureChain(clusterPolicyChain, tx, activeChains, skipServiceUpdate)
		}

		// localPolicyChain 存放 "Local" 策略使用的 Endpoint。
		localPolicyChain := svcInfo.localPolicyChainName
		usesLocalPolicyChain := len(localEndpoints) > 0 && svcInfo.UsesLocalEndpoints()
		if usesLocalPolicyChain {
			ensureChain(localPolicyChain, tx, activeChains, skipServiceUpdate)
		}

		// internalPolicyChain 存放 "internal"（ClusterIP）流量的 Endpoint；
		// internalTrafficChain 是内部流量跳转的目标（两者始终相同）。
		// hasInternalEndpoints 表示是否应生成指向 internalTrafficChain 的规则，
		// 即是否存在可用的内部 Endpoint。
		internalPolicyChain := clusterPolicyChain
		hasInternalEndpoints := hasEndpoints
		if svcInfo.InternalPolicyLocal() {
			internalPolicyChain = localPolicyChain
			if len(localEndpoints) == 0 {
				hasInternalEndpoints = false
			}
		}
		internalTrafficChain := internalPolicyChain

		// 类似地，externalPolicyChain 存放 "external"（NodePort、LoadBalancer、
		// ExternalIP）流量的 Endpoint；externalTrafficChain 是外部流量跳转的目标
		// （始终是该服务的 "EXT" chain）。hasExternalEndpoints 表示外部流量能否
		// 到达 Endpoint。即使没有外部 Endpoint，也可能需要生成 externalTrafficChain，
		// 以便设置本地流量的短路规则。
		externalPolicyChain := clusterPolicyChain
		hasExternalEndpoints := hasEndpoints
		if svcInfo.ExternalPolicyLocal() {
			externalPolicyChain = localPolicyChain
			if len(localEndpoints) == 0 {
				hasExternalEndpoints = false
			}
		}
		externalTrafficChain := svcInfo.externalChainName // eventually jumps to externalPolicyChain

		// usesExternalTrafficChain 依据 hasEndpoints 而非 hasExternalEndpoints，
		// 因为即使没有外部可用 Endpoint，也需要本地流量短路规则。
		usesExternalTrafficChain := hasEndpoints && svcInfo.ExternallyAccessible()
		if usesExternalTrafficChain {
			ensureChain(externalTrafficChain, tx, activeChains, skipServiceUpdate)
		}

		var internalTrafficFilterVerdict, externalTrafficFilterVerdict string
		if !hasEndpoints {
			// 服务完全无 Endpoint：internal/external 都为 false，不在 nat 表生成
			// 任何 chain，只在 filter 表用规则拒绝发往服务 IP 的报文。
			internalTrafficFilterVerdict = fmt.Sprintf("goto %s", rejectChain)
			externalTrafficFilterVerdict = fmt.Sprintf("goto %s", rejectChain)
		} else {
			if !hasInternalEndpoints {
				// internalTrafficPolicy 为 Local 但无本地 Endpoint：发往 clusterIP
				// 的流量被丢弃，外部流量仍可能被接受。
				internalTrafficFilterVerdict = "drop"
				serviceNoLocalEndpointsTotalInternal++
			}
			if !hasExternalEndpoints {
				// externalTrafficPolicy 为 Local 但无本地 Endpoint：集群外发往
				// "external" IP 的流量被丢弃，集群内流量仍可能被接受。
				externalTrafficFilterVerdict = "drop"
				serviceNoLocalEndpointsTotalExternal++
			}
		}

		// 记录 clusterIP。
		proxier.clusterIPs.ensureElem(tx, &knftables.Element{
			Set: clusterIPsSet,
			Key: []string{svcInfo.ClusterIP().String()},
		})
		if hasInternalEndpoints {
			proxier.serviceIPs.ensureElem(tx, &knftables.Element{
				Map: serviceIPsMap,
				Key: []string{
					svcInfo.ClusterIP().String(),
					protocol,
					strconv.Itoa(svcInfo.Port()),
				},
				Value: []string{
					fmt.Sprintf("goto %s", internalTrafficChain),
				},
			})
		} else {
			// 无 Endpoint。
			proxier.noEndpointServices.ensureElem(tx, &knftables.Element{
				Map: noEndpointServicesMap,
				Key: []string{
					svcInfo.ClusterIP().String(),
					protocol,
					strconv.Itoa(svcInfo.Port()),
				},
				Value: []string{
					internalTrafficFilterVerdict,
				},
				Comment: &svcPortComment,
			})
		}

		// 记录 externalIP。
		for _, externalIP := range svcInfo.ExternalIPs() {
			if hasEndpoints {
				// 发往 external IP 的流量跳转到 "external destinations" chain。
				proxier.serviceIPs.ensureElem(tx, &knftables.Element{
					Map: serviceIPsMap,
					Key: []string{
						externalIP.String(),
						protocol,
						strconv.Itoa(svcInfo.Port()),
					},
					Value: []string{
						fmt.Sprintf("goto %s", externalTrafficChain),
					},
				})
			}
			if !hasExternalEndpoints {
				// 要么完全没有 Endpoint（REJECT），要么没有外部流量可用的
				// Endpoint（DROP 所有未被 EXT chain 短路的流量）。
				proxier.noEndpointServices.ensureElem(tx, &knftables.Element{
					Map: noEndpointServicesMap,
					Key: []string{
						externalIP.String(),
						protocol,
						strconv.Itoa(svcInfo.Port()),
					},
					Value: []string{
						externalTrafficFilterVerdict,
					},
					Comment: &svcPortComment,
				})
			}
		}

		usesFWChain := len(svcInfo.LoadBalancerVIPs()) > 0 && len(svcInfo.LoadBalancerSourceRanges()) > 0
		fwChain := svcInfo.firewallChainName
		if usesFWChain {
			ensureChain(fwChain, tx, activeChains, skipServiceUpdate)
		}

		// 记录 load-balancer ingress。
		for _, lbip := range svcInfo.LoadBalancerVIPs() {
			if hasEndpoints {
				proxier.serviceIPs.ensureElem(tx, &knftables.Element{
					Map: serviceIPsMap,
					Key: []string{
						lbip.String(),
						protocol,
						strconv.Itoa(svcInfo.Port()),
					},
					Value: []string{
						fmt.Sprintf("goto %s", externalTrafficChain),
					},
				})
			}

			if usesFWChain {
				proxier.firewallIPs.ensureElem(tx, &knftables.Element{
					Map: firewallIPsMap,
					Key: []string{
						lbip.String(),
						protocol,
						strconv.Itoa(svcInfo.Port()),
					},
					Value: []string{
						fmt.Sprintf("goto %s", fwChain),
					},
				})
			}
		}
		if !hasExternalEndpoints {
			// 要么完全没有 Endpoint（REJECT），要么没有外部流量可用的
			// Endpoint（DROP 所有未被 EXT chain 短路的流量）。
			for _, lbip := range svcInfo.LoadBalancerVIPs() {
				proxier.noEndpointServices.ensureElem(tx, &knftables.Element{
					Map: noEndpointServicesMap,
					Key: []string{
						lbip.String(),
						protocol,
						strconv.Itoa(svcInfo.Port()),
					},
					Value: []string{
						externalTrafficFilterVerdict,
					},
					Comment: &svcPortComment,
				})
			}
		}

		// 记录 NodePort。
		if svcInfo.NodePort() != 0 {
			if proxier.localhostNodePortProxy != nil && svcInfo.Protocol() == v1.ProtocolTCP {
				eps := clusterEndpoints
				if svcInfo.ExternalPolicyLocal() {
					eps = localEndpoints
				}
				epStrs := make([]string, 0, len(eps))
				for _, ep := range eps {
					epStrs = append(epStrs, ep.String())
				}
				localhostNodePorts = append(localhostNodePorts, localnodeportproxy.NodePortSpec{
					ServicePortName:     svcName,
					Protocol:            svcInfo.Protocol(),
					NodePort:            svcInfo.NodePort(),
					Endpoints:           epStrs,
					SessionAffinityType: svcInfo.SessionAffinityType(),
					StickyMaxAgeSeconds: svcInfo.StickyMaxAgeSeconds(),
				})
			}
			if proxier.rejectsLocalhostNodePort(svcInfo.Protocol()) {
				proxier.localhostNodePortRejects.ensureElem(tx, &knftables.Element{
					Map: localhostNodePortRejectMap,
					Key: []string{
						protocol,
						strconv.Itoa(svcInfo.NodePort()),
					},
					Value: []string{
						fmt.Sprintf("goto %s", localhostNodePortRejectChainName(svcInfo.Protocol())),
					},
				})
			}
			if hasEndpoints {
				// 无论是好是坏，NodePort 不受 loadBalancerSourceRanges 约束，
				// 现有行为无法更改。
				proxier.serviceNodePorts.ensureElem(tx, &knftables.Element{
					Map: serviceNodePortsMap,
					Key: []string{
						protocol,
						strconv.Itoa(svcInfo.NodePort()),
					},
					Value: []string{
						fmt.Sprintf("goto %s", externalTrafficChain),
					},
				})
			}
			if !hasExternalEndpoints {
				// 要么完全没有 Endpoint（REJECT），要么没有外部流量可用的
				// Endpoint（DROP 所有未被 EXT chain 短路的流量）。
				proxier.noEndpointNodePorts.ensureElem(tx, &knftables.Element{
					Map: noEndpointNodePortsMap,
					Key: []string{
						protocol,
						strconv.Itoa(svcInfo.NodePort()),
					},
					Value: []string{
						externalTrafficFilterVerdict,
					},
					Comment: &svcPortComment,
				})
			}
		}

		// 以下操作都与 service chain 相关；服务及 Endpoint 无变化时全部跳过。
		if skipServiceUpdate {
			continue
		}

		// 设置外部流量处理（如果有 "external" 目的地）。所有外部目的地的流量
		// 都跳到 externalTrafficChain，它处理特殊情况后再跳到 externalPolicyChain。
		if usesExternalTrafficChain {
			if !svcInfo.ExternalPolicyLocal() {
				// 使用非本地 Endpoint 时必须 masquerade，因为流量可能跨节点。
				tx.Add(&knftables.Rule{
					Chain: externalTrafficChain,
					Rule: knftables.Concat(
						proxier.masqueradeRule,
					),
					Comment: ptr.To("masquerade"),
				})
			} else {
				// 只使用同节点 Endpoint 时，多数情况下可以保留源 IP。

				if proxier.localDetector.IsImplemented() {
					// 本节点 Pod 发往外部目的地的流量单独处理：不受任何流量策略
					// 约束，模拟先出到外部负载均衡器再从外部进来的路径。
					tx.Add(&knftables.Rule{
						Chain: externalTrafficChain,
						Rule: knftables.Concat(
							proxier.localDetector.IfLocalNFT(),
							"goto", clusterPolicyChain,
						),
						Comment: ptr.To("short-circuit pod traffic"),
					})
				}

				// 本机（非 Pod）发起的流量仍需 masquerade：LBIP 是本地地址，
				// 会被选作源 IP。
				tx.Add(&knftables.Rule{
					Chain: externalTrafficChain,
					Rule: knftables.Concat(
						"fib", "saddr", "type", "local",
						proxier.masqueradeRule,
					),
					Comment: ptr.To("masquerade local traffic"),
				})

				// 把 src-type=LOCAL 发往外部目的地的流量重定向到 policy=cluster
				// chain，使本机发起的流量也能正确到达服务。
				tx.Add(&knftables.Rule{
					Chain: externalTrafficChain,
					Rule: knftables.Concat(
						"fib", "saddr", "type", "local",
						"goto", clusterPolicyChain,
					),
					Comment: ptr.To("short-circuit local traffic"),
				})
			}

			// 其余流量落到相应的 policy chain。
			if hasExternalEndpoints {
				tx.Add(&knftables.Rule{
					Chain: externalTrafficChain,
					Rule: knftables.Concat(
						"goto", externalPolicyChain,
					),
				})
			}
		}

		if usesFWChain {
			var sources []string
			allowFromNode := false
			for _, cidr := range svcInfo.LoadBalancerSourceRanges() {
				if len(sources) > 0 {
					sources = append(sources, ",")
				}
				sources = append(sources, cidr.String())
				if cidr.Contains(proxier.nodeIP) {
					allowFromNode = true
				}
			}
			// 对 VIP 型 LB，VIP 常通过路由规则被配置成本地地址。此时节点
			// 访问 VIP 不会走到负载均衡器，而是回环并把源 IP 设为 VIP。
			// 因此需要下面的规则放行来自本节点的请求。
			if allowFromNode {
				for _, lbip := range svcInfo.LoadBalancerVIPs() {
					sources = append(sources, ",", lbip.String())
				}
			}
			tx.Add(&knftables.Rule{
				Chain: fwChain,
				Rule: knftables.Concat(
					ipX, "saddr", "!=", "{", sources, "}",
					"drop",
				),
			})
		}

		if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
			// 为每个 Endpoint 生成 affinity 集合。
			for _, ep := range allLocallyReachableEndpoints {
				epInfo, ok := ep.(*endpointInfo)
				if !ok {
					proxier.logger.Error(nil, "Failed to cast endpointsInfo", "endpointsInfo", ep)
					continue
				}

				// 创建存放当前 affinity 映射的集合。与 iptables 后端一致，
				// affinity 按源 IP（不区分源端口）到某个 ServicePort（不区分
				// 具体服务 IP）记录连接。今后可能改变这一行为。
				tx.Add(&knftables.Set{
					Name: epInfo.affinitySetName,
					Type: ipvX_addr,
					Flags: []knftables.SetFlag{
						// nft 文档说 "dynamic" 只对含状态对象（如 counter）的
						// 集合有必要，但至少在 RHEL8 上，不带 "dynamic" 创建的
						// 集合后续会被内核自动加上该标志，导致下次 tx.Add()
						// 因看起来在修改 flags 而失败。
						knftables.DynamicFlag,
						knftables.TimeoutFlag,
					},
					Timeout: ptr.To(time.Duration(svcInfo.StickyMaxAgeSeconds()) * time.Second),
				})
			}
		}

		// 写入 endpoint 规则/chain
		if !serviceUsesAffinity {
			if usesClusterPolicyChain {
				proxier.writeServiceToEndpointDNATs(tx, svcInfo, clusterPolicyChain, clusterEndpoints)
			}
			// 使用亲和性时，改写为跳到各 endpoint chain 的规则。
			if usesLocalPolicyChain {
				proxier.writeServiceToEndpointDNATs(tx, svcInfo, localPolicyChain, localEndpoints)
			}
		} else {
			if usesClusterPolicyChain {
				proxier.writeServiceToEndpointJumps(tx, svcInfo, clusterPolicyChain, clusterEndpoints)
			}
			if usesLocalPolicyChain {
				proxier.writeServiceToEndpointJumps(tx, svcInfo, localPolicyChain, localEndpoints)
			}

			// 生成每个 Endpoint 的 chain 和 affinity 集合
			for _, ep := range allLocallyReachableEndpoints {
				epInfo, ok := ep.(*endpointInfo)
				if !ok {
					continue
				}

				endpointChain := epInfo.chainName

				// 处理会话亲和性
				tx.Add(&knftables.Rule{
					Chain: endpointChain,
					Rule: knftables.Concat(
						"update", "@", epInfo.affinitySetName,
						"{", ipX, "saddr", "}",
					),
				})

				// DNAT 到最终目标。
				tx.Add(&knftables.Rule{
					Chain: endpointChain,
					Rule: knftables.Concat(
						"meta l4proto", protocol,
						"dnat to", epInfo.String(),
					),
				})
			}
		}
	}

	// 判断哪些 chain 已过期。不能立即删除：6.2 之前的内核在删除指向 chain 的
	// map 元素后，要过一小段时间才意识到该 chain 已无引用。因此这里先 flush，
	// 并把变旧的时间记入 staleChains，稍后再删除。
	for chain := range existingChains {
		if isServiceChainName(chain) {
			if !activeChains.Has(chain) {
				tx.Flush(&knftables.Chain{
					Name: chain,
				})
				proxier.staleChains[chain] = start
			} else {
				delete(proxier.staleChains, chain)
			}
		}
	}

	// 另一方面，过期的 affinity set 可以立即删除。
	for set := range existingAffinitySets {
		if !activeAffinitySets.Has(set) {
			tx.Delete(&knftables.Set{
				Name: set,
			})
		}
	}

	proxier.clusterIPs.cleanupLeftoverKeys(tx)
	proxier.serviceIPs.cleanupLeftoverKeys(tx)
	proxier.firewallIPs.cleanupLeftoverKeys(tx)
	proxier.noEndpointServices.cleanupLeftoverKeys(tx)
	proxier.noEndpointNodePorts.cleanupLeftoverKeys(tx)
	proxier.serviceNodePorts.cleanupLeftoverKeys(tx)
	proxier.hairpinConnections.cleanupLeftoverKeys(tx)
	if proxier.localhostNodePortsEnabled {
		proxier.localhostNodePortRejects.cleanupLeftoverKeys(tx)
	}

	// 同步规则。
	proxier.logger.V(2).Info("Reloading service nftables data",
		"numServices", len(proxier.svcPortMap),
		"numEndpoints", totalEndpoints,
	)

	if klogV9 := klog.V(9); klogV9.Enabled() {
		klogV9.InfoS("Running nftables transaction", "transaction", tx.String())
	}

	err = proxier.nftables.Run(context.TODO(), tx)
	if err != nil {
		proxier.logger.Error(err, "nftables sync failed")
		metrics.NFTablesSyncFailuresTotal.WithLabelValues(string(proxier.ipFamily)).Inc()

		// 事务未执行成功，staleChains 的记录已不准确（chain 并未被 flush）。
		// 下一轮重新计算即可。
		clear(proxier.staleChains)
		// 打印失败的事务并列出完整的 kube-proxy table。
		proxier.logFailure(tx)
		return
	}
	success = true
	proxier.needFullSync = false

	for name, lastChangeTriggerTimes := range endpointUpdateResult.LastChangeTriggerTimes {
		for _, lastChangeTriggerTime := range lastChangeTriggerTimes {
			latency := metrics.SinceInSeconds(lastChangeTriggerTime)
			metrics.NetworkProgrammingLatency.WithLabelValues(string(proxier.ipFamily)).Observe(latency)
			proxier.logger.V(4).Info("Network programming", "endpoint", klog.KRef(name.Namespace, name.Name), "elapsed", latency)
		}
	}

	metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("internal", string(proxier.ipFamily)).Set(float64(serviceNoLocalEndpointsTotalInternal))
	metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("external", string(proxier.ipFamily)).Set(float64(serviceNoLocalEndpointsTotalExternal))
	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}
	metrics.SyncProxyRulesLastTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()

	// 更新服务健康检查。Endpoint 列表可能包含非 "OnlyLocal" 服务，但服务列表
	// 不会包含它们，serviceHealthServer 会直接忽略这些 Endpoint。
	if err := proxier.serviceHealthServer.SyncServices(proxier.svcPortMap.HealthCheckNodePorts()); err != nil {
		proxier.logger.Error(err, "Error syncing healthcheck services")
	}
	if err := proxier.serviceHealthServer.SyncEndpoints(proxier.endpointsMap.LocalReadyEndpoints()); err != nil {
		proxier.logger.Error(err, "Error syncing healthcheck endpoints")
	}

	if proxier.localhostNodePortProxy != nil {
		proxier.localhostNodePortProxy.SyncNodePorts(localhostNodePorts)
	}

	if endpointUpdateResult.ConntrackCleanupRequired {
		// 收尾工作：清理 UDP Service 的过期 conntrack 条目。
		conntrack.CleanStaleEntries(proxier.conntrack, proxier.ipFamily, proxier.svcPortMap, proxier.endpointsMap)
	}
	return
}

// writeServiceToEndpointDNATs 在 svcChain 中写入到各 Endpoint 的 DNAT 规则。
func (proxier *Proxier) writeServiceToEndpointDNATs(tx *knftables.Transaction, svcInfo *servicePortInfo, svcChain string, endpoints []proxy.Endpoint) {
	ipX := "ip"
	if proxier.ipFamily == v1.IPv6Protocol {
		ipX = "ip6"
	}

	// 单 Endpoint 服务特殊处理：向 nftables 插入 map 是 O(n) 甚至更差的操作，
	// 单 Endpoint 时直接用规则 DNAT，避免引入 map。
	if len(endpoints) == 1 {
		ep := endpoints[0]
		epInfo, ok := ep.(*endpointInfo)
		if !ok {
			return
		}

		tx.Add(&knftables.Rule{
			Chain: svcChain,
			Rule: knftables.Concat(
				"meta l4proto", strings.ToLower(string(svcInfo.Protocol())),
				"dnat to", net.JoinHostPort(epInfo.IP(), strconv.Itoa(epInfo.Port())),
			),
		})

		return
	}

	var elements []string
	for i, ep := range endpoints {
		epInfo, ok := ep.(*endpointInfo)
		if !ok {
			continue
		}

		elements = append(elements,
			strconv.Itoa(i), ":", epInfo.IP(), ".", strconv.Itoa(epInfo.Port()),
		)
		// numgen map 元素之间需要逗号分隔，最后一个除外。
		if i != len(endpoints)-1 {
			elements = append(elements, ",")
		}
	}
	tx.Add(&knftables.Rule{
		Chain: svcChain,
		Rule: knftables.Concat(
			"meta l4proto", strings.ToLower(string(svcInfo.Protocol())),
			"dnat", ipX, "addr . port to",
			"numgen random mod", len(endpoints), "map",
			"{", elements, "}",
		),
	})
}

// writeServiceToEndpointJumps 在启用会话亲和性时写入到每 Endpoint chain 的跳转规则。
func (proxier *Proxier) writeServiceToEndpointJumps(tx *knftables.Transaction, svcInfo *servicePortInfo, svcChain string, endpoints []proxy.Endpoint) {
	ipX := "ip"
	if proxier.ipFamily == v1.IPv6Protocol {
		ipX = "ip6"
	}

	var elements []string
	// 写入 affinity 规则，构造 vmap 元素
	for i, ep := range endpoints {
		epInfo, ok := ep.(*endpointInfo)
		if !ok {
			continue
		}

		tx.Add(&knftables.Rule{
			Chain: svcChain,
			Rule: knftables.Concat(
				ipX, "saddr", "@", epInfo.affinitySetName,
				"goto", epInfo.chainName,
			),
		})

		elements = append(elements,
			strconv.Itoa(i), ":", "goto", epInfo.chainName,
		)
		if i != len(endpoints)-1 {
			elements = append(elements, ",")
		}
	}
	// 没有 affinity 规则命中时走这个 vmap。
	tx.Add(&knftables.Rule{
		Chain: svcChain,
		Rule: knftables.Concat(
			"numgen random mod", len(endpoints), "vmap",
			"{", elements, "}",
		),
	})
}
