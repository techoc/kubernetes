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

package iptables

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/events"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/conntrack"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	"k8s.io/kubernetes/pkg/proxy/metaproxier"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/kubernetes/pkg/proxy/runner"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	"k8s.io/kubernetes/pkg/proxy/util/nfacct"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
)

const (
	// KUBE-SERVICES 链：Service 流量的主入口链。
	kubeServicesChain utiliptables.Chain = "KUBE-SERVICES"

	// KUBE-EXTERNAL-SERVICES 链：集群外可见的 Service 流量入口链。
	kubeExternalServicesChain utiliptables.Chain = "KUBE-EXTERNAL-SERVICES"

	// KUBE-NODEPORTS 链：NodePort 流量入口链。
	kubeNodePortsChain utiliptables.Chain = "KUBE-NODEPORTS"

	// KUBE-POSTROUTING 链：kube-proxy 的 POSTROUTING 处理链。
	kubePostroutingChain utiliptables.Chain = "KUBE-POSTROUTING"

	// kubeMarkMasqChain 是 mark-for-masquerade 链：给需要 SNAT 的包打标记。
	kubeMarkMasqChain utiliptables.Chain = "KUBE-MARK-MASQ"

	// KUBE-FORWARD 链：kube-proxy 的 FORWARD 处理链。
	kubeForwardChain utiliptables.Chain = "KUBE-FORWARD"

	// kubeProxyFirewallChain 是 kube-proxy 的防火墙链。
	kubeProxyFirewallChain utiliptables.Chain = "KUBE-PROXY-FIREWALL"

	// kube-proxy canary 链，用于监控规则重载。
	kubeProxyCanaryChain utiliptables.Chain = "KUBE-PROXY-CANARY"

	// kubeletFirewallChain 是 kubelet 防火墙链的副本，包含反 martian 包规则。
	// 不能用于其他任何规则。
	kubeletFirewallChain utiliptables.Chain = "KUBE-FIREWALL"

	// largeClusterEndpointsThreshold 是切换到“大规模集群模式”的端点数量阈值。
	// 达到该阈值后优先保证 iptables 性能，而非规则的可读性/可调试性。
	largeClusterEndpointsThreshold = 1000
)

const sysctlRouteLocalnet = "net/ipv4/conf/all/route_localnet"
const sysctlNFConntrackTCPBeLiberal = "net/netfilter/nf_conntrack_tcp_be_liberal"

// NewDualStackProxier 创建一个 MetaProxier 实例，同时包含 IPv4 和 IPv6 代理。
func NewDualStackProxier(
	ctx context.Context,
	config *kubeproxyconfig.KubeProxyConfiguration,
	ipts map[v1.IPFamily]utiliptables.Interface,
	sysctl utilsysctl.Interface,
	localDetectors map[v1.IPFamily]proxyutil.LocalTrafficDetector,
	nodeName string,
	nodeIPs map[v1.IPFamily]net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
	initOnly bool,
) (proxy.Provider, error) {
	// 先创建 IPv4 的单栈 Proxier。
	// 创建单栈 Proxier 的 ipv4 实例
	ipv4Proxier, err := NewProxier(ctx, config, v1.IPv4Protocol, ipts[v1.IPv4Protocol], sysctl,
		localDetectors[v1.IPv4Protocol], nodeName, nodeIPs[v1.IPv4Protocol],
		recorder, healthzServer, initOnly)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv4 proxier: %v", err)
	}

	ipv6Proxier, err := NewProxier(ctx, config, v1.IPv6Protocol, ipts[v1.IPv6Protocol], sysctl,
		localDetectors[v1.IPv6Protocol], nodeName, nodeIPs[v1.IPv6Protocol],
		recorder, healthzServer, initOnly)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv6 proxier: %v", err)
	}
	if initOnly {
		return nil, nil
	}
	return metaproxier.NewMetaProxier(ipv4Proxier, ipv6Proxier), nil
}

// Proxier 是基于 iptables 的代理实现。
type Proxier struct {
	// ipFamily 表示该 Proxier 所跟踪的 IP 协议族。
	ipFamily v1.IPFamily

	// endpointsChanges 和 serviceChanges 保存自上次 iptables 同步以来端点和 Service 的所有变更。
	// 对单个对象，变更会被累积：previous 是所有这些变更前的状态，current 是应用全部变更后的状态。
	endpointsChanges *proxy.EndpointsChangeTracker
	serviceChanges   *proxy.ServiceChangeTracker

	mu             sync.Mutex // protects the following fields
	svcPortMap     proxy.ServicePortMap
	endpointsMap   proxy.EndpointsMap
	topologyLabels map[string]string
	// endpointSlicesSynced 和 servicesSynced 在启动时同步完对应对象后置为 true。
	// 避免 kube-proxy 重启后用部分数据更新 iptables。
	endpointSlicesSynced bool
	servicesSynced       bool
	lastFullSync         time.Time
	needFullSync         bool
	initialized          int32
	syncRunner           *runner.BoundedFrequencyRunner // governs calls to syncProxyRules
	syncPeriod           time.Duration
	lastIPTablesCleanup  time.Time

	// 以下字段实际上是常量，不需要持有互斥锁即可访问。
	iptables       utiliptables.Interface
	masqueradeAll  bool
	masqueradeMark string
	conntrack      conntrack.Interface
	nfacct         nfacct.Interface
	localDetector  proxyutil.LocalTrafficDetector
	nodeName       string
	nodeIP         net.IP

	serviceHealthServer healthcheck.ServiceHealthServer
	healthzServer       *healthcheck.ProxyHealthServer

	// 将概率（浮点数）转换为字符串开销较大，而我们只使用 1/n 形式的概率，
	// 因此预先计算并缓存一批概率值供后续复用。
	precomputedProbabilities []string

	// 以下缓冲区用于复用内存、避免分配，这些分配对性能影响显著。
	iptablesData             *bytes.Buffer
	existingFilterChainsData *bytes.Buffer
	filterChains             proxyutil.LineBuffer
	filterRules              proxyutil.LineBuffer
	natChains                proxyutil.LineBuffer
	natRules                 proxyutil.LineBuffer

	// largeClusterMode 在 syncProxyRules 开头设置：若本次将输出“大量”iptables 规则，
	// 则优先优化性能而不是可调试性。
	largeClusterMode bool

	// localhostNodePorts 表示是否允许通过 localhost 访问 NodePort Service。
	localhostNodePorts bool

	// conntrackTCPLiberal 表示系统是否设置了内核参数 nf_conntrack_tcp_be_liberal。
	conntrackTCPLiberal bool

	// nodePortAddresses 选择 NodePort 生效的网络接口。
	nodePortAddresses *proxyutil.NodePortAddresses
	// networkInterfacer 定义若干 net 库函数的接口，
	// 便于在测试中注入。
	networkInterfacer proxyutil.NetworkInterfacer

	logger klog.Logger

	// nfAcctCounters 用于判断 nfacct 子系统中计数器是否存在。
	nfAcctCounters map[string]bool
}

// Proxier 实现了 proxy.Provider 接口。
var _ proxy.Provider = &Proxier{}

// NewProxier 返回一个新的单栈 IPTables Proxier。
func NewProxier(ctx context.Context,
	config *kubeproxyconfig.KubeProxyConfiguration,
	ipFamily v1.IPFamily,
	ipt utiliptables.Interface,
	sysctl utilsysctl.Interface,
	localDetector proxyutil.LocalTrafficDetector,
	nodeName string,
	nodeIP net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
	initOnly bool,
) (*Proxier, error) {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "ipFamily", ipFamily)

	localhostNodePorts := *config.IPTables.LocalhostNodePorts
	nodePortAddresses := proxyutil.NewNodePortAddresses(ipFamily, config.NodePortAddresses)
	if !nodePortAddresses.ContainsIPv4Loopback() {
		localhostNodePorts = false
	}
	if localhostNodePorts {
		// 在 loopback 地址上暴露 NodePort 需要设置 route_localnet sysctl。
		// 参考 https://issues.k8s.io/90259
		// 在 loopback 地址上暴露 NodePort 需要设置 route_localnet sysctl。
		// 参考 https://issues.k8s.io/90259
		logger.Info("Setting route_localnet=1 to allow node-ports on localhost; to change this either disable iptables.localhostNodePorts (--iptables-localhost-nodeports) or set nodePortAddresses (--nodeport-addresses) to filter loopback addresses")
		if err := proxyutil.EnsureSysctl(sysctl, sysctlRouteLocalnet, 1); err != nil {
			return nil, err
		}
	}

	// 自己的行为要保守，对他人的行为要宽容。
	// 若该 sysctl 非零，则只将窗口外的 RST 报文标记为 INVALID。
	// 参考 https://docs.kernel.org/networking/nf_conntrack-sysctl.html
	// 自己的行为要保守，对他人的行为要宽容。
	// 若该 sysctl 非零，则只将窗口外的 RST 报文标记为 INVALID。
	// 参考 https://docs.kernel.org/networking/nf_conntrack-sysctl.html
	conntrackTCPLiberal := false
	if val, err := sysctl.GetSysctl(sysctlNFConntrackTCPBeLiberal); err == nil && val != 0 {
		conntrackTCPLiberal = true
		logger.Info("nf_conntrack_tcp_be_liberal set, not installing DROP rules for INVALID packets")
	}

	if initOnly {
		logger.Info("System initialized and --init-only specified")
		return nil, nil
	}

	// 生成用于 SNAT 规则的 masquerade 标记。
	masqueradeValue := 1 << uint(*config.IPTables.MasqueradeBit)
	masqueradeMark := fmt.Sprintf("%#08x", masqueradeValue)
	logger.V(2).Info("Using iptables mark for masquerade", "mark", masqueradeMark)

	serviceHealthServer := healthcheck.NewServiceHealthServer(nodeName, recorder, nodePortAddresses, healthzServer, ipFamily)
	nfacctRunner, err := nfacct.New()
	if err != nil {
		logger.Error(err, "Failed to create nfacct runner, nfacct based metrics won't be available")
	}

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
		iptables:                 ipt,
		masqueradeAll:            config.Linux.MasqueradeAll,
		masqueradeMark:           masqueradeMark,
		conntrack:                conntrack.New(),
		nfacct:                   nfacctRunner,
		localDetector:            localDetector,
		nodeName:                 nodeName,
		nodeIP:                   nodeIP,
		serviceHealthServer:      serviceHealthServer,
		healthzServer:            healthzServer,
		precomputedProbabilities: make([]string, 0, 1001),
		iptablesData:             bytes.NewBuffer(nil),
		existingFilterChainsData: bytes.NewBuffer(nil),
		filterChains:             proxyutil.NewLineBuffer(),
		filterRules:              proxyutil.NewLineBuffer(),
		natChains:                proxyutil.NewLineBuffer(),
		natRules:                 proxyutil.NewLineBuffer(),
		localhostNodePorts:       localhostNodePorts,
		nodePortAddresses:        nodePortAddresses,
		networkInterfacer:        proxyutil.RealNetwork{},
		conntrackTCPLiberal:      conntrackTCPLiberal,
		logger:                   logger,
		nfAcctCounters: map[string]bool{
			metrics.IPTablesCTStateInvalidDroppedNFAcctCounter: false,
			metrics.LocalhostNodePortAcceptedNFAcctCounter:     false,
		},
	}

	logger.V(2).Info("Iptables sync params", "minSyncPeriod", minSyncPeriod, "syncPeriod", syncPeriod, "maxSyncPeriod", proxyutil.FullSyncPeriod)
	// 将 syncPeriod 传给 ipt.Monitor，它只在需要时才回调我们。
	// 但 NewBoundedFrequencyRunner 仍必须传入某个 maxInterval；
	// time.Hour 是任意选取的值。
	// 将 syncPeriod 传给 ipt.Monitor，它只在需要时才回调我们。
	// 但无论如何都必须给 NewBoundedFrequencyRunner 传入某个 maxInterval。
	// time.Hour 是任意选取的值。
	proxier.syncRunner = runner.NewBoundedFrequencyRunner("sync-runner", proxier.syncProxyRules, minSyncPeriod, syncPeriod, proxyutil.FullSyncPeriod)

	go ipt.Monitor(kubeProxyCanaryChain, []utiliptables.Table{utiliptables.TableMangle, utiliptables.TableNAT, utiliptables.TableFilter},
		proxier.forceSyncProxyRules, syncPeriod, wait.NeverStop)

	return proxier, nil
}

// servicePortInfo 是保存 Service 端口字符串信息的内部结构体。
type servicePortInfo struct {
	*proxy.BaseServicePortInfo
	// 以下字段出于性能考虑预先计算并存储。
	nameString             string
	clusterPolicyChainName utiliptables.Chain
	localPolicyChainName   utiliptables.Chain
	firewallChainName      utiliptables.Chain
	externalChainName      utiliptables.Chain
}

// newServiceInfo 返回一个包装 serviceInfo 的 proxy.ServicePort。
func newServiceInfo(port *v1.ServicePort, service *v1.Service, bsvcPortInfo *proxy.BaseServicePortInfo) proxy.ServicePort {
	svcPort := &servicePortInfo{BaseServicePortInfo: bsvcPortInfo}

	// 出于性能考虑预先存储以下信息。
	svcName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	svcPortName := proxy.ServicePortName{NamespacedName: svcName, Port: port.Name}
	protocol := strings.ToLower(string(svcPort.Protocol()))
	svcPort.nameString = svcPortName.String()
	svcPort.clusterPolicyChainName = servicePortPolicyClusterChain(svcPort.nameString, protocol)
	svcPort.localPolicyChainName = servicePortPolicyLocalChainName(svcPort.nameString, protocol)
	svcPort.firewallChainName = serviceFirewallChainName(svcPort.nameString, protocol)
	svcPort.externalChainName = serviceExternalChainName(svcPort.nameString, protocol)

	return svcPort
}

// endpointInfo 是保存端点信息的内部结构体。
type endpointInfo struct {
	*proxy.BaseEndpointInfo

	ChainName utiliptables.Chain
}

// newEndpointInfo 返回一个包装 endpointInfo 的 proxy.Endpoint。
func newEndpointInfo(baseInfo *proxy.BaseEndpointInfo, svcPortName *proxy.ServicePortName) proxy.Endpoint {
	return &endpointInfo{
		BaseEndpointInfo: baseInfo,
		ChainName:        servicePortEndpointChainName(svcPortName.String(), strings.ToLower(string(svcPortName.Protocol)), baseInfo.String()),
	}
}

type iptablesJumpChain struct {
	table     utiliptables.Table
	dstChain  utiliptables.Chain
	srcChain  utiliptables.Chain
	comment   string
	extraArgs []string
}

var iptablesJumpChains = []iptablesJumpChain{
	{utiliptables.TableFilter, kubeExternalServicesChain, utiliptables.ChainInput, "kubernetes externally-visible service portals", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableFilter, kubeExternalServicesChain, utiliptables.ChainForward, "kubernetes externally-visible service portals", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableFilter, kubeNodePortsChain, utiliptables.ChainInput, "kubernetes health check service ports", nil},
	{utiliptables.TableFilter, kubeServicesChain, utiliptables.ChainForward, "kubernetes service portals", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableFilter, kubeServicesChain, utiliptables.ChainOutput, "kubernetes service portals", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableFilter, kubeForwardChain, utiliptables.ChainForward, "kubernetes forwarding rules", nil},
	{utiliptables.TableFilter, kubeProxyFirewallChain, utiliptables.ChainInput, "kubernetes load balancer firewall", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableFilter, kubeProxyFirewallChain, utiliptables.ChainOutput, "kubernetes load balancer firewall", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableFilter, kubeProxyFirewallChain, utiliptables.ChainForward, "kubernetes load balancer firewall", []string{"-m", "conntrack", "--ctstate", "NEW"}},
	{utiliptables.TableNAT, kubeServicesChain, utiliptables.ChainOutput, "kubernetes service portals", nil},
	{utiliptables.TableNAT, kubeServicesChain, utiliptables.ChainPrerouting, "kubernetes service portals", nil},
	{utiliptables.TableNAT, kubePostroutingChain, utiliptables.ChainPostrouting, "kubernetes postrouting rules", nil},
}

// 这些链与 pkg/kubelet/kubelet_network_linux.go 中创建的链重复；
// 启动时创建，但 CleanupLeftovers 中不会删除。
// 与 pkg/kubelet/kubelet_network_linux.go 中创建的链重复；
// 启动时创建这些链，但 CleanupLeftovers 中不会删除。
var iptablesKubeletJumpChains = []iptablesJumpChain{
	{utiliptables.TableFilter, kubeletFirewallChain, utiliptables.ChainInput, "", nil},
	{utiliptables.TableFilter, kubeletFirewallChain, utiliptables.ChainOutput, "", nil},
}

// 从 iptablesJumpChains 中移除链时，将其加入此处，
// 以便升级时被清理。
// 从 iptablesJumpChains 中移除链时，将其加入此处，
// 以便升级时被清理。
var iptablesCleanupOnlyChains = []iptablesJumpChain{}

// computeProbability 计算 1/n 的概率字符串，保留 10 位小数。
// iptables 的 --probability 接受十进制小数，固定 10 位精度既足够精确又能统一格式，便于缓存。
func computeProbability(n int) string {
	return fmt.Sprintf("%0.10f", 1.0/float64(n))
}

// 调用方须持有 proxier.mu。
func (proxier *Proxier) precomputeProbabilities(numberOfPrecomputed int) {
	if len(proxier.precomputedProbabilities) == 0 {
		proxier.precomputedProbabilities = append(proxier.precomputedProbabilities, "<bad value>")
	}
	for i := len(proxier.precomputedProbabilities); i <= numberOfPrecomputed; i++ {
		proxier.precomputedProbabilities = append(proxier.precomputedProbabilities, computeProbability(i))
	}
}

// 调用方须持有 proxier.mu。
func (proxier *Proxier) probability(n int) string {
	if n >= len(proxier.precomputedProbabilities) {
		proxier.precomputeProbabilities(n)
	}
	return proxier.precomputedProbabilities[n]
}

// Sync 被调用以尽快将 Proxier 状态同步到 iptables。
func (proxier *Proxier) Sync() {
	if proxier.healthzServer != nil {
		proxier.healthzServer.QueuedUpdate(proxier.ipFamily)
	}
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Run()
}

// SyncLoop 执行周期性工作，预期作为 goroutine 或应用主循环运行，不会返回。
func (proxier *Proxier) SyncLoop() {
	// 一开始就更新 healthz 时间戳，以防 Sync() 从未成功。
	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}

	// informer 同步期间，合成“上次变更入队”时间。
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Loop(wait.NeverStop)

}

// setInitialized 原子地设置初始化标志。
// 使用 int32 原子变量而非 bool，是为了让事件回调无锁读取。
func (proxier *Proxier) setInitialized(value bool) {
	var initialized int32
	if value {
		initialized = 1
	}
	atomic.StoreInt32(&proxier.initialized, initialized)
}

// isInitialized 判断 Service 和 EndpointSlice 是否都已完成初始同步。
// 在此之前不同步规则，避免用不完整数据覆盖 iptables。
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

// OnServiceSynced 在所有初始事件处理器执行完毕、状态完全传播到本地缓存后调用一次。
func (proxier *Proxier) OnServiceSynced() {
	proxier.mu.Lock()
	proxier.servicesSynced = true
	proxier.setInitialized(proxier.endpointSlicesSynced)
	proxier.mu.Unlock()

	// 无条件同步——该方法在进程生命周期内只调用一次。
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

// OnEndpointSlicesSynced 在所有初始事件处理器执行完毕、状态完全传播到本地缓存后调用一次。
func (proxier *Proxier) OnEndpointSlicesSynced() {
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.setInitialized(proxier.servicesSynced)
	proxier.mu.Unlock()

	// 无条件同步——该方法在进程生命周期内只调用一次。
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

// OnServiceCIDRsChanged 在任一 ServiceCIDR 发生变化时调用，参数提供完整的 service CIDR 列表。
// OnServiceCIDRsChanged 在任一 ServiceCIDR 发生变化时调用，
// 并提供完整的 service CIDR 列表。
func (proxier *Proxier) OnServiceCIDRsChanged(_ []string) {}

// portProtoHash 根据 ServicePortName 和协议返回 16 字符的哈希。
// 计算方式：先做 sha256 哈希，再 base32 编码，最后截取前 16 个字符。
// iptables 链名长度必须 <= 28 字符，且链名越长越难阅读，因此需要短哈希。
// portProtoHash 根据 ServicePortName 和协议返回 16 字符的哈希。
// 计算方式：先做 sha256 哈希，
// 再编码为 base32 并截取前 16 个字符；这样做是因为 iptables
// 链名必须 <= 28 字符，且越长越难阅读。
func portProtoHash(servicePortName string, protocol string) string {
	hash := sha256.Sum256([]byte(servicePortName + protocol))
	encoded := base32.StdEncoding.EncodeToString(hash[:])
	return encoded[:16]
}

const (
	servicePortPolicyClusterChainNamePrefix = "KUBE-SVC-"
	servicePortPolicyLocalChainNamePrefix   = "KUBE-SVL-"
	serviceFirewallChainNamePrefix          = "KUBE-FW-"
	serviceExternalChainNamePrefix          = "KUBE-EXT-"
	servicePortEndpointChainNamePrefix      = "KUBE-SEP-"
)

// servicePortPolicyClusterChain 返回某 Service 的 KUBE-SVC-XXXX 链名。
// 这是该 Service 的主 iptables 链，使用 `Cluster` 流量策略时用于将流量分发到端点。
// servicePortPolicyClusterChain 返回某 Service 的 KUBE-SVC-XXXX 链名，这是该
// Service 的主 iptables 链，使用 `Cluster` 流量策略时用于将流量分发
// 到端点。
func servicePortPolicyClusterChain(servicePortName string, protocol string) utiliptables.Chain {
	return utiliptables.Chain(servicePortPolicyClusterChainNamePrefix + portProtoHash(servicePortName, protocol))
}

// servicePortPolicyLocalChainName 返回某 Service 的 KUBE-SVL-XXXX 链名。
// 使用 `Local` 流量策略时负责分发到本地端点。
// 仅当 Service 的内部或外部流量策略为 `Local` 时该链才存在。
// servicePortPolicyLocalChainName 返回某 Service 的 KUBE-SVL-XXXX 链名，
// 使用 `Local` 流量策略时负责分发到本地端点；仅当 Service 的内部
// 或外部流量策略为 `Local` 时该链才存在。
func servicePortPolicyLocalChainName(servicePortName string, protocol string) utiliptables.Chain {
	return utiliptables.Chain(servicePortPolicyLocalChainNamePrefix + portProtoHash(servicePortName, protocol))
}

// serviceFirewallChainName 返回某 Service 的 KUBE-FW-XXXX 链名，
// 用于实现 LoadBalancerSourceRanges 特性的过滤。
// serviceFirewallChainName 返回某 Service 的 KUBE-FW-XXXX 链名，
// 用于实现 LoadBalancerSourceRanges 特性的过滤。
func serviceFirewallChainName(servicePortName string, protocol string) utiliptables.Chain {
	return utiliptables.Chain(serviceFirewallChainNamePrefix + portProtoHash(servicePortName, protocol))
}

// serviceExternalChainName 返回某 Service 的 KUBE-EXT-XXXX 链名。
// 使用 `Local` 外部流量策略时，它对集群内部发起、目的为外部地址的流量做“短路”处理：
// 将本地来源的流量转发到 KUBE-SVC-XXXX 链，外部来源的流量转发到 KUBE-SVL-XXXX 链。
// serviceExternalChainName 返回某 Service 的 KUBE-EXT-XXXX 链名，
// 使用 `Local` 外部流量策略时对集群内部发起、目的为外部地址的流量做“短路”：
// 将本地来源的流量转发到 KUBE-SVC-XXXX 链，
// 将外部来源的流量转发到 KUBE-SVL-XXXX 链。
func serviceExternalChainName(servicePortName string, protocol string) utiliptables.Chain {
	return utiliptables.Chain(serviceExternalChainNamePrefix + portProtoHash(servicePortName, protocol))
}

// servicePortEndpointChainName 返回某个 Service 端点的 KUBE-SEP-XXXX 链名。
func servicePortEndpointChainName(servicePortName string, protocol string, endpoint string) utiliptables.Chain {
	hash := sha256.Sum256([]byte(servicePortName + protocol + endpoint))
	encoded := base32.StdEncoding.EncodeToString(hash[:])
	return utiliptables.Chain(servicePortEndpointChainNamePrefix + encoded[:16])
}

// isServiceChainName 判断链名是否属于 kube-proxy 为 Service 生成的链。
// 这些前缀是生成规则时约定的固定前缀，因此可用前缀匹配作为启发式判断：
// 删除过期链时据此只处理自己创建的链，避免误删其他组件的链。
func isServiceChainName(chainString string) bool {
	prefixes := []string{
		servicePortPolicyClusterChainNamePrefix,
		servicePortPolicyLocalChainNamePrefix,
		servicePortEndpointChainNamePrefix,
		serviceFirewallChainNamePrefix,
		serviceExternalChainNamePrefix,
	}

	for _, p := range prefixes {
		if strings.HasPrefix(chainString, p) {
			return true
		}
	}
	return false
}

// 调用方须持有 proxier.mu。
func (proxier *Proxier) appendServiceCommentLocked(args []string, svcName string) []string {
	// 不输出这些注释可使 iptables 规则体积（端点数量很大时）缩减 40% 以上。
	// 因此端点链足够多时直接丢弃注释。
	if proxier.largeClusterMode {
		return args
	}
	return append(args, "-m", "comment", "--comment", svcName)
}

// 由 iptables.Monitor 调用，或响应拓扑变化；
// 它会调用 syncProxyRules()，告诉它重新同步所有 Service，不论 Service 或 Endpoints/EndpointSlice 对象本身是否变化。
// 由 iptables.Monitor 调用，或响应拓扑变化；它调用 syncProxyRules()，
// 告诉它重新同步所有 Service，不论 Service 或 Endpoints/EndpointSlice
// 对象本身是否发生变化。
func (proxier *Proxier) forceSyncProxyRules() {
	proxier.mu.Lock()
	proxier.needFullSync = true
	proxier.mu.Unlock()

	proxier.syncProxyRules()
}

// 所有 iptables-save/restore 调用都在这里进行。
// 其余 iptables 规则仅在 iptablesInit() 中设置。
// 调用方不应持有 proxier.mu。
// 所有 iptables-save/restore 调用都在这里进行。
// 其余 iptables 规则仅在 iptablesInit() 中设置。
// 调用方不应持有 proxier.mu。
func (proxier *Proxier) syncProxyRules() (retryError error) {
	proxier.mu.Lock()
	defer proxier.mu.Unlock()

	// 收到 Service 和端点之前不同步规则。
	if !proxier.isInitialized() {
		proxier.logger.V(2).Info("Not syncing iptables until Services and Endpoints have been received from master")
		return
	}

	// 记录同步耗时。
	start := time.Now()

	doFullSync := proxier.needFullSync ||
		// 大规模集群避免频繁的完全同步。
		((time.Since(proxier.lastFullSync) > proxyutil.FullSyncPeriod) && !proxier.largeClusterMode)

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

	proxier.logger.V(2).Info("Syncing iptables rules", "fullSync", doFullSync)

	success := false
	defer func() {
		if !success {
			proxier.logger.Info("Sync failed", "retryingTime", proxier.syncPeriod)
			retryError = fmt.Errorf("Sync failed")
			if !doFullSync {
				metrics.IPTablesPartialRestoreFailuresTotal.WithLabelValues(string(proxier.ipFamily)).Inc()
			}
			// proxier.serviceChanges 和 proxier.endpointChanges 已被清空，
			// 执行增量同步所需的状态已经丢失，因此必须改为完全同步。
			// proxier.serviceChanges 和 proxier.endpointChanges 已被清空，
			// 增量同步所需的状态已经丢失，
			// 因此必须改为完全同步。
			proxier.needFullSync = true
		} else if doFullSync {
			proxier.lastFullSync = time.Now()
		}
	}()

	if doFullSync {
		// 确保跳转规则（例如从 PREROUTING 到 KUBE-SERVICES）存在。
		// 这些规则不能放在 iptables-restore 中，因为我们不想指定/替换 PREROUTING 中的所有规则。
		//
		// kube-proxy 首次启动、以及 utiliptables Monitor 检测到 iptables 被清空时，需要创建/重建这些规则；
		// 这两种情况都会强制完全同步。其他情况下可假定规则已存在，增量同步跳过此步骤，
		// 避免每次同步调用 20 次 /sbin/iptables（在规则很多的主机上会非常慢）。
		for _, jump := range append(iptablesJumpChains, iptablesKubeletJumpChains...) {
			if _, err := proxier.iptables.EnsureChain(jump.table, jump.dstChain); err != nil {
				proxier.logger.Error(err, "Failed to ensure chain exists", "table", jump.table, "chain", jump.dstChain)
				return
			}
			args := jump.extraArgs
			if jump.comment != "" {
				args = append(args, "-m", "comment", "--comment", jump.comment)
			}
			args = append(args, "-j", string(jump.dstChain))
			if _, err := proxier.iptables.EnsureRule(utiliptables.Prepend, jump.table, jump.srcChain, args...); err != nil {
				proxier.logger.Error(err, "Failed to ensure chain jumps", "table", jump.table, "srcChain", jump.srcChain, "dstChain", jump.dstChain)
				return
			}
		}

		// 确保 nfacct 计数器存在。
		if proxier.nfacct != nil {
			for name := range proxier.nfAcctCounters {
				if err := proxier.nfacct.Ensure(name); err != nil {
					proxier.nfAcctCounters[name] = false
					proxier.logger.Error(err, "Failed to create nfacct counter; the corresponding metric will not be updated", "counter", name)
				} else {
					proxier.nfAcctCounters[name] = true
				}
			}
		}
	}

	// 在此之后，直到尝试写入 iptables 规则之前都不会返回。

	// 重置所有后续使用的缓冲区，避免内存重新分配以提升性能。
	proxier.filterChains.Reset()
	proxier.filterRules.Reset()
	proxier.natChains.Reset()
	proxier.natRules.Reset()

	skippedNatChains := proxyutil.NewDiscardLineBuffer()
	skippedNatRules := proxyutil.NewDiscardLineBuffer()

	// 为所有将要填充的“顶层”链写入链定义行。
	for _, chainName := range []utiliptables.Chain{kubeServicesChain, kubeExternalServicesChain, kubeForwardChain, kubeNodePortsChain, kubeProxyFirewallChain} {
		proxier.filterChains.Write(utiliptables.MakeChainLine(chainName))
	}
	for _, chainName := range []utiliptables.Chain{kubeServicesChain, kubeNodePortsChain, kubePostroutingChain, kubeMarkMasqChain} {
		proxier.natChains.Write(utiliptables.MakeChainLine(chainName))
	}

	// 安装 Kubernetes 专用的 postrouting 规则。使用整条链便于清空和修改，
	// 例如标记值变化时。
	// 安装 Kubernetes 专用的 postrouting 规则。这里使用整条链，
	// 是为了便于清空和修改，例如标记值
	// 需要变更时。

	proxier.natRules.Write(
		"-A", string(kubePostroutingChain),
		"-m", "mark", "!", "--mark", fmt.Sprintf("%s/%s", proxier.masqueradeMark, proxier.masqueradeMark),
		"-j", "RETURN",
	)
	// 清除标记，避免数据包重新经过网络栈时被再次伪装（masquerade）。
	// 清除标记，避免报文重新经过网络栈时被再次 masquerade。
	proxier.natRules.Write(
		"-A", string(kubePostroutingChain),
		"-j", "MARK", "--xor-mark", proxier.masqueradeMark,
	)
	proxier.natRules.Write(
		"-A", string(kubePostroutingChain),
		"-m", "comment", "--comment", `"kubernetes service traffic requiring SNAT"`,
		"-j", "MASQUERADE",
		"--random-fully",
	)

	// 安装 Kubernetes 专用的 masquerade 标记规则。使用整条链便于清空和修改，
	// 例如标记值变化时。
	// 安装 Kubernetes 专用的 masquerade 标记规则。这里使用整条链，
	// 是为了便于清空和修改，例如标记值
	// 需要变更时。
	proxier.natRules.Write(
		"-A", string(kubeMarkMasqChain),
		"-j", "MARK", "--or-mark", proxier.masqueradeMark,
	)

	isIPv6 := proxier.iptables.IsIPv6()
	if !isIPv6 && proxier.localhostNodePorts {
		// Kube-proxy 使用 route_localnet 在 localhost 上启用 NodePort 会带来安全漏洞
		// （https://issue.k8s.io/90259），该 iptables 规则用于缓解它。
		// Kube-proxy 使用 route_localnet 在 localhost 上启用 NodePort 会带来
		// 安全漏洞（https://issue.k8s.io/90259），这条 iptables 规则
		// 用于缓解它。

		// 注意：kubelet 会创建一条完全相同的规则。以后修改此规则时，
		// 必须保证与 kubelet 创建的旧版本规则正确互操作。
		// （实际上 kubelet 使用 "--dst"/"--src" 而非 "-d"/"-s"，但这只是命令行差异，
		// 在内核中生成的是同一条规则。）
		// 注意：kubelet 会创建一条完全相同的规则。若将来要修改
		// 此规则，必须保证与 kubelet 创建的旧版本规则能正确互操作。
		// （实际上 kubelet 使用 "--dst"/"--src" 而非 "-d"/"-s"，
		// 但这只是命令行层面的差异，
		// 最终在内核中生成的规则
		// 完全相同。）
		proxier.filterChains.Write(utiliptables.MakeChainLine(kubeletFirewallChain))
		proxier.filterRules.Write(
			"-A", string(kubeletFirewallChain),
			"-m", "comment", "--comment", `"block incoming localnet connections"`,
			"-d", "127.0.0.0/8",
			"!", "-s", "127.0.0.0/8",
			"-m", "conntrack",
			"!", "--ctstate", "RELATED,ESTABLISHED,DNAT",
			"-j", "DROP",
		)
	}

	// 记录需要保留的 NAT 链。
	activeNATChains := sets.New[utiliptables.Chain]()

	// 为避免切片增长，任意设定其容量为 64：单行规则的参数不会超过这个数量。
	// 超过 64 也仍然正确——这只是效率问题，不影响正确性。
	// 为避免切片增长，任意设定其容量为 64：
	// 单行规则的参数不会超过这个数量。
	// 注意，即使超过 64 也仍然正确——
	// 这只是效率问题，不影响正确性。
	args := make([]string, 64)

	// 统计所有 Service 的端点总数，用于判断集群规模。
	// 统计所有 Service 的端点链总数，
	// 用于判断集群规模。
	totalEndpoints := 0
	for svcName := range proxier.svcPortMap {
		totalEndpoints += len(proxier.endpointsMap[svcName])
	}
	proxier.largeClusterMode = (totalEndpoints > largeClusterEndpointsThreshold)

	// 这两个变量用于上报 sync_proxy_rules_no_endpoints_total 指标。
	serviceNoLocalEndpointsTotalInternal := 0
	serviceNoLocalEndpointsTotalExternal := 0

	// 为每个 Service 端口构建规则。
	for svcName, svc := range proxier.svcPortMap {
		svcInfo, ok := svc.(*servicePortInfo)
		if !ok {
			proxier.logger.Error(nil, "Failed to cast serviceInfo", "serviceName", svcName)
			continue
		}
		protocol := strings.ToLower(string(svcInfo.Protocol()))
		svcPortNameString := svcInfo.nameString

		// 分别确定 Cluster 和 Local 流量策略对应的端点。
		// allLocallyReachableEndpoints 是在给定流量策略下可从本节点路由到的所有端点；
		// hasEndpoints 表示 Service 在任意节点（不只是本节点）上是否有可用端点。
		allEndpoints := proxier.endpointsMap[svcName]
		clusterEndpoints, localEndpoints, allLocallyReachableEndpoints, hasEndpoints := proxy.CategorizeEndpoints(allEndpoints, svcInfo, proxier.nodeName, proxier.topologyLabels)

		// clusterPolicyChain 包含 `Cluster` 流量策略使用的端点。
		clusterPolicyChain := svcInfo.clusterPolicyChainName
		usesClusterPolicyChain := len(clusterEndpoints) > 0 && svcInfo.UsesClusterEndpoints()

		// localPolicyChain 包含 `Local` 流量策略使用的端点。
		localPolicyChain := svcInfo.localPolicyChainName
		usesLocalPolicyChain := len(localEndpoints) > 0 && svcInfo.UsesLocalEndpoints()

		// internalPolicyChain 是包含“内部”（ClusterIP）流量端点的链；
		// internalTrafficChain 是内部流量被路由到的链（与 internalPolicyChain 相同）。
		// hasInternalEndpoints 表示是否应生成指向 internalTrafficChain 的规则，即是否存在可用的内部端点。
		// internalPolicyChain 是包含“内部”（ClusterIP）流量
		// 端点的链；internalTrafficChain 是内部流量被路由
		// 到的链（始终与 internalPolicyChain 相同）。
		// hasInternalEndpoints 为 true 表示应生成指向
		// internalTrafficChain 的规则；没有可用的内部端点时
		// 为 false。
		internalPolicyChain := clusterPolicyChain
		hasInternalEndpoints := hasEndpoints
		if svcInfo.InternalPolicyLocal() {
			internalPolicyChain = localPolicyChain
			if len(localEndpoints) == 0 {
				hasInternalEndpoints = false
			}
		}
		internalTrafficChain := internalPolicyChain

		// 类似地，externalPolicyChain 包含“外部”（NodePort、LoadBalancer、ExternalIP）流量的端点；
		// externalTrafficChain 是外部流量被路由到的链（始终是 Service 的 EXT 链）。
		// hasExternalEndpoints 表示是否有外部流量能够到达的端点。
		// 但即使没有外部端点，也可能需要生成 externalTrafficChain，以设置本地流量的短路规则。
		// 类似地，externalPolicyChain 包含“外部”
		// （NodePort、LoadBalancer 和 ExternalIP）流量的端点。
		// externalTrafficChain 是外部流量被路由到的链
		// （始终是 Service 的 EXT 链）。hasExternalEndpoints 为 true
		// 表示存在外部流量能到达的端点。
		// （但即使没有外部端点，也可能需要生成
		// externalTrafficChain，以确保本地流量的短路规则
		// 被设置。）
		externalPolicyChain := clusterPolicyChain
		hasExternalEndpoints := hasEndpoints
		if svcInfo.ExternalPolicyLocal() {
			externalPolicyChain = localPolicyChain
			if len(localEndpoints) == 0 {
				hasExternalEndpoints = false
			}
		}
		externalTrafficChain := svcInfo.externalChainName // eventually jumps to externalPolicyChain

		// usesExternalTrafficChain 基于 hasEndpoints 而非 hasExternalEndpoints：
		// 即使没有外部可用端点，也需要本地流量短路规则。
		// usesExternalTrafficChain 基于 hasEndpoints 而非
		// hasExternalEndpoints，因为即使没有外部可用端点，
		// 也需要本地流量短路规则。
		usesExternalTrafficChain := hasEndpoints && svcInfo.ExternallyAccessible()

		// 到 LoadBalancer IP 的流量通常可直接进入 externalTrafficChain；
		// 但若使用了 LoadBalancerSourceRanges，则需要创建防火墙链。
		// 到 LoadBalancer IP 的流量通常可直接进入 externalTrafficChain，
		// 但若使用了 LoadBalancerSourceRanges，则需要
		// 创建防火墙链。
		loadBalancerTrafficChain := externalTrafficChain
		fwChain := svcInfo.firewallChainName
		usesFWChain := hasEndpoints && len(svcInfo.LoadBalancerVIPs()) > 0 && len(svcInfo.LoadBalancerSourceRanges()) > 0
		if usesFWChain {
			loadBalancerTrafficChain = fwChain
		}

		var internalTrafficFilterTarget, internalTrafficFilterComment string
		var externalTrafficFilterTarget, externalTrafficFilterComment string
		if !hasEndpoints {
			// Service 完全没有任何端点；hasInternalEndpoints 和 hasExternalEndpoints 也为 false。
			// 此时不会在 nat 表中为该 Service 生成任何链，只在 filter 表中生成拒绝到达 Service IP 的规则。
			// 该 Service 完全没有端点；hasInternalEndpoints 和
			// hasExternalEndpoints 也为 false；我们不会在 nat 表中
			// 为该 Service 生成任何链，只会在 filter 表中生成
			// 拒绝发往该 Service IP 的入站报文的
			// 规则。
			internalTrafficFilterTarget = "REJECT"
			internalTrafficFilterComment = fmt.Sprintf(`"%s has no endpoints"`, svcPortNameString)
			externalTrafficFilterTarget = "REJECT"
			externalTrafficFilterComment = internalTrafficFilterComment
		} else {
			if !hasInternalEndpoints {
				// internalTrafficPolicy 为 `Local` 但没有本地端点：到 clusterIP 的流量被丢弃，
				// 但外部流量仍可能被接受。
				// internalTrafficPolicy 为 `Local` 但没有本地端点：
				// 到 clusterIP 的流量会被丢弃，
				// 但外部流量仍可能被接受。
				internalTrafficFilterTarget = "DROP"
				internalTrafficFilterComment = fmt.Sprintf(`"%s has no local endpoints"`, svcPortNameString)
				serviceNoLocalEndpointsTotalInternal++
			}
			if !hasExternalEndpoints {
				// externalTrafficPolicy 为 `Local` 但没有本地端点：集群外到“外部”IP 的流量被丢弃，
				// 但集群内流量仍可能被接受。
				// externalTrafficPolicy 为 `Local` 但没有本地端点：
				// 集群外发往“外部”IP 的流量会被丢弃，
				// 但集群内流量仍可能被
				// 接受。
				externalTrafficFilterTarget = "DROP"
				externalTrafficFilterComment = fmt.Sprintf(`"%s has no local endpoints"`, svcPortNameString)
				serviceNoLocalEndpointsTotalExternal++
			}
		}

		filterRules := proxier.filterRules
		natChains := proxier.natChains
		natRules := proxier.natRules

		// 捕获 clusterIP 流量。
		if hasInternalEndpoints {
			natRules.Write(
				"-A", string(kubeServicesChain),
				"-m", "comment", "--comment", fmt.Sprintf(`"%s cluster IP"`, svcPortNameString),
				"-m", protocol, "-p", protocol,
				"-d", svcInfo.ClusterIP().String(),
				"--dport", strconv.Itoa(svcInfo.Port()),
				"-j", string(internalTrafficChain))
		} else {
			// 没有端点。
			filterRules.Write(
				"-A", string(kubeServicesChain),
				"-m", "comment", "--comment", internalTrafficFilterComment,
				"-m", protocol, "-p", protocol,
				"-d", svcInfo.ClusterIP().String(),
				"--dport", strconv.Itoa(svcInfo.Port()),
				"-j", internalTrafficFilterTarget,
			)
		}

		// 捕获 externalIP 流量。
		for _, externalIP := range svcInfo.ExternalIPs() {
			if hasEndpoints {
				// 将发往 external IP 的流量导向“外部目的地”链。
				natRules.Write(
					"-A", string(kubeServicesChain),
					"-m", "comment", "--comment", fmt.Sprintf(`"%s external IP"`, svcPortNameString),
					"-m", protocol, "-p", protocol,
					"-d", externalIP.String(),
					"--dport", strconv.Itoa(svcInfo.Port()),
					"-j", string(externalTrafficChain))
			}
			if !hasExternalEndpoints {
				// 要么完全没有端点（REJECT），要么没有外部流量可用的端点
				// （丢弃所有未被 EXT 链短路的流量）。
				filterRules.Write(
					"-A", string(kubeExternalServicesChain),
					"-m", "comment", "--comment", externalTrafficFilterComment,
					"-m", protocol, "-p", protocol,
					"-d", externalIP.String(),
					"--dport", strconv.Itoa(svcInfo.Port()),
					"-j", externalTrafficFilterTarget,
				)
			}
		}

		// 捕获 load-balancer 入口流量。
		for _, lbip := range svcInfo.LoadBalancerVIPs() {
			if hasEndpoints {
				natRules.Write(
					"-A", string(kubeServicesChain),
					"-m", "comment", "--comment", fmt.Sprintf(`"%s loadbalancer IP"`, svcPortNameString),
					"-m", protocol, "-p", protocol,
					"-d", lbip.String(),
					"--dport", strconv.Itoa(svcInfo.Port()),
					"-j", string(loadBalancerTrafficChain))

			}
			if usesFWChain {
				filterRules.Write(
					"-A", string(kubeProxyFirewallChain),
					"-m", "comment", "--comment", fmt.Sprintf(`"%s traffic not accepted by %s"`, svcPortNameString, svcInfo.firewallChainName),
					"-m", protocol, "-p", protocol,
					"-d", lbip.String(),
					"--dport", strconv.Itoa(svcInfo.Port()),
					"-j", "DROP")
			}
		}
		if !hasExternalEndpoints {
			// 要么完全没有端点（REJECT），要么没有外部流量可用的端点
			// （丢弃所有未被 EXT 链短路的流量）。
			// 要么完全没有端点（REJECT），要么没有外部
			// 流量可用的端点：丢弃所有未被 EXT 链短路
			// 的流量。
			for _, lbip := range svcInfo.LoadBalancerVIPs() {
				filterRules.Write(
					"-A", string(kubeExternalServicesChain),
					"-m", "comment", "--comment", externalTrafficFilterComment,
					"-m", protocol, "-p", protocol,
					"-d", lbip.String(),
					"--dport", strconv.Itoa(svcInfo.Port()),
					"-j", externalTrafficFilterTarget,
				)
			}
		}

		// 捕获 NodePort 流量。
		if svcInfo.NodePort() != 0 {
			if hasEndpoints {
				// 跳转到外部目的地链。无论好坏，NodePort 不受 loadBalancerSourceRanges 限制，
				// 这一点无法改变。
				// 跳转到外部目的地链。无论好坏，NodePort 都不受
				// loadBalancerSourceRanges 限制，
				// 这一点无法改变。
				if proxier.localhostNodePorts && proxier.ipFamily == v1.IPv4Protocol && proxier.nfAcctCounters[metrics.LocalhostNodePortAcceptedNFAcctCounter] {
					natRules.Write(
						"-A", string(kubeNodePortsChain),
						"-m", "comment", "--comment", svcPortNameString,
						"-m", protocol, "-p", protocol,
						"-d", "127.0.0.0/8",
						"--dport", strconv.Itoa(svcInfo.NodePort()),
						"-m", "nfacct", "--nfacct-name", metrics.LocalhostNodePortAcceptedNFAcctCounter,
						"-j", string(externalTrafficChain))
				}
				natRules.Write(
					"-A", string(kubeNodePortsChain),
					"-m", "comment", "--comment", svcPortNameString,
					"-m", protocol, "-p", protocol,
					"--dport", strconv.Itoa(svcInfo.NodePort()),
					"-j", string(externalTrafficChain))
			}
			if !hasExternalEndpoints {
				// 要么完全没有端点（REJECT），要么没有外部流量可用的端点
				// （丢弃所有未被 EXT 链短路的流量）。
				// 要么完全没有端点（REJECT），要么没有外部
				// 流量可用的端点：丢弃所有未被 EXT 链短路
				// 的流量。
				filterRules.Write(
					"-A", string(kubeExternalServicesChain),
					"-m", "comment", "--comment", externalTrafficFilterComment,
					"-m", "addrtype", "--dst-type", "LOCAL",
					"-m", protocol, "-p", protocol,
					"--dport", strconv.Itoa(svcInfo.NodePort()),
					"-j", externalTrafficFilterTarget,
				)
			}
		}

		// 捕获 healthCheckNodePort 流量。
		if svcInfo.HealthCheckNodePort() != 0 {
			// 无论节点上是否有本地端点，healthCheckNodePort 都需要一条规则接受传入连接。
			filterRules.Write(
				"-A", string(kubeNodePortsChain),
				"-m", "comment", "--comment", fmt.Sprintf(`"%s health check node port"`, svcPortNameString),
				"-m", "tcp", "-p", "tcp",
				"--dport", strconv.Itoa(svcInfo.HealthCheckNodePort()),
				"-j", "ACCEPT",
			)
		}

		// 若 SVC/SVL/EXT/FW/SEP 链自上次同步以来未变化，可从 restore 输入中省略它们。
		// 但为了指标正确，仍需计算本来会生成的链数量，因此照常计算后丢弃结果。
		// 若 SVC/SVL/EXT/FW/SEP 链自上次同步以来未变化，
		// 可从 restore 输入中省略它们。但为了指标正确，
		// 仍需算出本来会写入多少条链，
		// 因此照常计算后将结果丢弃。
		if !doFullSync && !serviceUpdateResult.UpdatedServices.Has(svcName.NamespacedName) && !endpointUpdateResult.UpdatedServices.Has(svcName.NamespacedName) {
			natChains = skippedNatChains
			natRules = skippedNatRules
		}

		// 设置内部流量处理规则。
		if hasInternalEndpoints {
			args = append(args[:0],
				"-m", "comment", "--comment", fmt.Sprintf(`"%s cluster IP"`, svcPortNameString),
				"-m", protocol, "-p", protocol,
				"-d", svcInfo.ClusterIP().String(),
				"--dport", strconv.Itoa(svcInfo.Port()),
			)
			if proxier.masqueradeAll {
				natRules.Write(
					"-A", string(internalTrafficChain),
					args,
					"-j", string(kubeMarkMasqChain))
			} else if proxier.localDetector.IsImplemented() {
				// 这是对服务 VIP 的集群外流量做 masquerade：可给 Service 网段配置静态路由，
				// 任意节点收到后都会代你桥接到 Service。由于流量可能离开节点再回来，因此需要在此伪装源地址。
				// 这是对服务 VIP 的集群外流量做 masquerade：可给
				// Service 网段配置一条静态路由，指向任意节点，
				// 该节点会代为桥接到 Service。由于流量可能离开
				// 节点再回来，因此需要在此伪装
				// 源地址。
				natRules.Write(
					"-A", string(internalTrafficChain),
					args,
					proxier.localDetector.IfNotLocal(),
					"-j", string(kubeMarkMasqChain))
			}
		}

		// 设置外部流量处理规则（若启用了“外部”目的地）。
		// 所有外部目的地的流量都应先跳转到 externalTrafficChain，由其处理特殊情况，
		// 再跳转到 externalPolicyChain。
		// 设置外部流量处理规则（若启用了“外部”目的地）。
		// 所有外部目的地的捕获流量都应跳转到
		// externalTrafficChain，由其处理特殊情况，
		// 再跳转到 externalPolicyChain。
		if usesExternalTrafficChain {
			natChains.Write(utiliptables.MakeChainLine(externalTrafficChain))
			activeNATChains.Insert(externalTrafficChain)

			if !svcInfo.ExternalPolicyLocal() {
				// 使用非本地端点时需要进行伪装，因为流量可能跨节点。
				natRules.Write(
					"-A", string(externalTrafficChain),
					"-m", "comment", "--comment", fmt.Sprintf(`"masquerade traffic for %s external destinations"`, svcPortNameString),
					"-j", string(kubeMarkMasqChain))
			} else {
				// 只使用同节点端点时，大多数情况下可以保留源 IP。

				if proxier.localDetector.IsImplemented() {
					// 将本地 Pod 发往外部目的地的流量作为特例：两种流量策略都不适用，
					// 模拟先出到外部负载均衡器再回来的过程。
					// 将本地 Pod 发往外部目的地的流量作为特例：
					// 两种流量策略都不适用于它，
					// 这模拟了先出到外部负载均衡器
					// 再回来的过程。
					natRules.Write(
						"-A", string(externalTrafficChain),
						"-m", "comment", "--comment", fmt.Sprintf(`"pod traffic for %s external destinations"`, svcPortNameString),
						proxier.localDetector.IfLocal(),
						"-j", string(clusterPolicyChain))
				}

				// 本地发起的流量（不是 Pod，而是主机节点）仍需要伪装，
				// 因为 LBIP 本身就是本地地址，会被选作源 IP。
				// 本地发起的流量（不是 Pod，而是主机节点）
				// 仍需要伪装，因为 LBIP 本身就是本地地址，
				// 会被选作源 IP。
				natRules.Write(
					"-A", string(externalTrafficChain),
					"-m", "comment", "--comment", fmt.Sprintf(`"masquerade LOCAL traffic for %s external destinations"`, svcPortNameString),
					"-m", "addrtype", "--src-type", "LOCAL",
					"-j", string(kubeMarkMasqChain))

				// 将所有 src-type=LOCAL 发往外部目的地的流量重定向到 policy=cluster 链，
				// 使主机发起的流量能被正确导向 Service。
				// 将所有 src-type=LOCAL 发往外部目的地的流量
				// 重定向到 policy=cluster 链，使主机发起的流量
				// 能被正确导向 Service。
				natRules.Write(
					"-A", string(externalTrafficChain),
					"-m", "comment", "--comment", fmt.Sprintf(`"route LOCAL traffic for %s external destinations"`, svcPortNameString),
					"-m", "addrtype", "--src-type", "LOCAL",
					"-j", string(clusterPolicyChain))
			}

			// 其余流量落到相应的策略链。
			if hasExternalEndpoints {
				natRules.Write(
					"-A", string(externalTrafficChain),
					"-j", string(externalPolicyChain))
			}
		}

		// 按需设置防火墙链。
		if usesFWChain {
			natChains.Write(utiliptables.MakeChainLine(fwChain))
			activeNATChains.Insert(fwChain)

			// Service 防火墙规则基于 loadBalancerSourceRanges 字段创建。
			// 这只适用于保留源 IP 的 VIP 型负载均衡器；
			// 对于直接将流量导向 Service NodePort 的负载均衡器，防火墙规则不适用。
			args = append(args[:0],
				"-A", string(fwChain),
				"-m", "comment", "--comment", fmt.Sprintf(`"%s loadbalancer IP"`, svcPortNameString),
			)

			// 按每个源地址范围做防火墙过滤。
			allowFromNode := false
			for _, cidr := range svcInfo.LoadBalancerSourceRanges() {
				natRules.Write(args, "-s", cidr.String(), "-j", string(externalTrafficChain))
				if cidr.Contains(proxier.nodeIP) {
					allowFromNode = true
				}
			}
			// 对 VIP 型 LB，VIP 常通过 IP 路由规则被添加为本地地址。
			// 此时节点访问 VIP 的流量不会经过负载均衡器，而是以 VIP 为源 IP 环回，
			// 因此需要以下规则允许来自本节点的请求。
			// 对 VIP 型 LB，VIP 常通过 IP 路由规则被添加
			// 为本地地址。此时节点访问 VIP 的流量不会
			// 经过负载均衡器，而是以 VIP 为源 IP 环回；
			// 因此需要以下规则允许来自本节点的
			// 请求。
			if allowFromNode {
				for _, lbip := range svcInfo.LoadBalancerVIPs() {
					natRules.Write(
						args,
						"-s", lbip.String(),
						"-j", string(externalTrafficChain))
				}
			}
			// 如果报文能够走到防火墙链末尾，说明它没有被 DNAT，
			// 因而会命中对应的 KUBE-PROXY-FIREWALL 规则。
			// 若报文能走到防火墙链末尾，说明它没有被 DNAT，
			// 因而会命中对应的
			// KUBE-PROXY-FIREWALL 规则。
			natRules.Write(
				"-A", string(fwChain),
				"-m", "comment", "--comment", fmt.Sprintf(`"other traffic to %s will be dropped by KUBE-PROXY-FIREWALL"`, svcPortNameString),
			)
		}

		// 如果使用 Cluster 策略，创建相应链及从 clusterPolicyChain 跳转到 clusterEndpoints 的规则。
		if usesClusterPolicyChain {
			natChains.Write(utiliptables.MakeChainLine(clusterPolicyChain))
			activeNATChains.Insert(clusterPolicyChain)
			proxier.writeServiceToEndpointRules(natRules, svcPortNameString, svcInfo, clusterPolicyChain, clusterEndpoints, args)
		}

		// 如果使用 Local 策略，创建相应链及从 localPolicyChain 跳转到 localEndpoints 的规则。
		if usesLocalPolicyChain {
			natChains.Write(utiliptables.MakeChainLine(localPolicyChain))
			activeNATChains.Insert(localPolicyChain)
			proxier.writeServiceToEndpointRules(natRules, svcPortNameString, svcInfo, localPolicyChain, localEndpoints, args)
		}

		// 为每个端点生成独立的链。
		for _, ep := range allLocallyReachableEndpoints {
			epInfo, ok := ep.(*endpointInfo)
			if !ok {
				proxier.logger.Error(nil, "Failed to cast endpointInfo", "endpointInfo", ep)
				continue
			}

			endpointChain := epInfo.ChainName

			// 创建端点链。
			natChains.Write(utiliptables.MakeChainLine(endpointChain))
			activeNATChains.Insert(endpointChain)

			args = append(args[:0], "-A", string(endpointChain))
			args = proxier.appendServiceCommentLocked(args, svcPortNameString)
			// 通过 SNAT 处理回环到发送方自身的流量。
			natRules.Write(
				args,
				"-s", epInfo.IP(),
				"-j", string(kubeMarkMasqChain))
			// 更新客户端亲和性（session affinity）列表。
			if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
				args = append(args, "-m", "recent", "--name", string(endpointChain), "--set")
			}
			// DNAT 到最终目的地。
			args = append(args, "-m", protocol, "-p", protocol, "-j", "DNAT", "--to-destination", epInfo.String())
			natRules.Write(args)
		}
	}

	// 删除不再使用的链。由于“iptables-save”在规则很多的主机上可能耗时数秒，
	// 大规模集群不需要每次同步都做此事。（过期链不会被任何活跃规则引用，
	// 除了占用内存外没有危害。）
	// 删除不再使用的链。由于“iptables-save”在规则很多的主机上
	// 可能耗时数秒，大规模集群不必每次同步都做此事。
	// （过期链不会被任何活跃规则引用，
	// 除了占用内存外没有危害。）
	deletedChains := 0
	if !proxier.largeClusterMode || time.Since(proxier.lastIPTablesCleanup) > proxier.syncPeriod {
		proxier.iptablesData.Reset()
		if err := proxier.iptables.SaveInto(utiliptables.TableNAT, proxier.iptablesData); err == nil {
			existingNATChains := utiliptables.GetChainsFromTable(proxier.iptablesData.Bytes())
			for chain := range existingNATChains.Difference(activeNATChains) {
				chainString := string(chain)
				if !isServiceChainName(chainString) {
					// 忽略不属于我们的链。
					continue
				}
				// 按照 iptables 的要求必须先为该链写一行链定义，这恰好会清空该链；
				// 之后再删除链。
				proxier.natChains.Write(utiliptables.MakeChainLine(chain))
				proxier.natRules.Write("-X", chainString)
				deletedChains++
			}
			proxier.lastIPTablesCleanup = time.Now()
		} else {
			proxier.logger.Error(err, "Failed to execute iptables-save: stale chains will not be deleted")
		}
	}

	// 最后尾调用 nodePorts 链，必须放在所有其他 Service 入口规则之后。
	if proxier.nodePortAddresses.MatchAll() {
		destinations := []string{"-m", "addrtype", "--dst-type", "LOCAL"}
		// 不支持时屏蔽 localhost NodePort。（IPv6 永远不支持；IPv4 仅在之前设置了 route_localnet 时支持。）
		if isIPv6 {
			destinations = append(destinations, "!", "-d", "::1/128")
		} else if !proxier.localhostNodePorts {
			destinations = append(destinations, "!", "-d", "127.0.0.0/8")
		}

		proxier.natRules.Write(
			"-A", string(kubeServicesChain),
			"-m", "comment", "--comment", `"kubernetes service nodeports; NOTE: this must be the last rule in this chain"`,
			destinations,
			"-j", string(kubeNodePortsChain))
	} else {
		nodeIPs, err := proxier.nodePortAddresses.GetNodeIPs(proxier.networkInterfacer)
		if err != nil {
			proxier.logger.Error(err, "Failed to get node ip address matching nodeport cidrs, services with nodeport may not work as intended", "CIDRs", proxier.nodePortAddresses)
		}
		for _, ip := range nodeIPs {
			if ip.IsLoopback() {
				if isIPv6 {
					proxier.logger.Error(nil, "--nodeport-addresses includes localhost but localhost NodePorts are not supported on IPv6", "address", ip.String())
					continue
				} else if !proxier.localhostNodePorts {
					proxier.logger.Error(nil, "--nodeport-addresses includes localhost but --iptables-localhost-nodeports=false was passed", "address", ip.String())
					continue
				}
			}

			// 逐个 IP 创建 NodePort 规则。
			proxier.natRules.Write(
				"-A", string(kubeServicesChain),
				"-m", "comment", "--comment", `"kubernetes service nodeports; NOTE: this must be the last rule in this chain"`,
				"-d", ip.String(),
				"-j", string(kubeNodePortsChain))
		}
	}

	// 丢弃处于 INVALID 状态的报文：若未设置 nf_conntrack_tcp_be_liberal，
	// 这些报文可能导致意外的连接重置。
	// 参考 https://github.com/kubernetes/kubernetes/issues/74839
	// 参考 https://github.com/kubernetes/kubernetes/issues/117924
	// 丢弃处于 INVALID 状态的报文：若未设置
	// nf_conntrack_tcp_be_liberal，这些报文可能导致意外的连接重置。
	// 参考 https://github.com/kubernetes/kubernetes/issues/74839
	// 参考 https://github.com/kubernetes/kubernetes/issues/117924
	if !proxier.conntrackTCPLiberal {
		rule := []string{
			"-A", string(kubeForwardChain),
			"-m", "conntrack",
			"--ctstate", "INVALID",
		}
		if proxier.nfAcctCounters[metrics.IPTablesCTStateInvalidDroppedNFAcctCounter] {
			rule = append(rule,
				"-m", "nfacct", "--nfacct-name", metrics.IPTablesCTStateInvalidDroppedNFAcctCounter,
			)
		}
		rule = append(rule,
			"-j", "DROP",
		)
		proxier.filterRules.Write(rule)
	}

	// 若报文带有 masqueradeMark，我们也希望转发同样的流量：
	// 即使 FORWARD 默认策略不是 ACCEPT，NodePort 流量也能被转发。
	// 若报文带有 masqueradeMark，我们也希望转发同样的流量：
	// 这样即使 FORWARD 默认策略不是 ACCEPT，
	// NodePort 流量也能被转发。
	proxier.filterRules.Write(
		"-A", string(kubeForwardChain),
		"-m", "comment", "--comment", `"kubernetes forwarding rules"`,
		"-m", "mark", "--mark", fmt.Sprintf("%s/%s", proxier.masqueradeMark, proxier.masqueradeMark),
		"-j", "ACCEPT",
	)

	// 下面这条规则确保被上面“kubernetes forwarding rules”规则放行的连接的后续报文也被接受。
	proxier.filterRules.Write(
		"-A", string(kubeForwardChain),
		"-m", "comment", "--comment", `"kubernetes forwarding conntrack rule"`,
		"-m", "conntrack",
		"--ctstate", "RELATED,ESTABLISHED",
		"-j", "ACCEPT",
	)

	metrics.IPTablesRulesTotal.WithLabelValues(string(utiliptables.TableFilter), string(proxier.ipFamily)).Set(float64(proxier.filterRules.Lines()))
	metrics.IPTablesRulesLastSync.WithLabelValues(string(utiliptables.TableFilter), string(proxier.ipFamily)).Set(float64(proxier.filterRules.Lines()))
	metrics.IPTablesRulesTotal.WithLabelValues(string(utiliptables.TableNAT), string(proxier.ipFamily)).Set(float64(proxier.natRules.Lines() + skippedNatRules.Lines() - deletedChains))
	metrics.IPTablesRulesLastSync.WithLabelValues(string(utiliptables.TableNAT), string(proxier.ipFamily)).Set(float64(proxier.natRules.Lines() - deletedChains))

	// 同步规则。
	proxier.iptablesData.Reset()
	proxier.iptablesData.WriteString("*filter\n")
	proxier.iptablesData.Write(proxier.filterChains.Bytes())
	proxier.iptablesData.Write(proxier.filterRules.Bytes())
	proxier.iptablesData.WriteString("COMMIT\n")
	proxier.iptablesData.WriteString("*nat\n")
	proxier.iptablesData.Write(proxier.natChains.Bytes())
	proxier.iptablesData.Write(proxier.natRules.Bytes())
	proxier.iptablesData.WriteString("COMMIT\n")

	proxier.logger.V(2).Info("Reloading service iptables data",
		"numServices", len(proxier.svcPortMap),
		"numEndpoints", totalEndpoints,
		"numFilterChains", proxier.filterChains.Lines(),
		"numFilterRules", proxier.filterRules.Lines(),
		"numNATChains", proxier.natChains.Lines(),
		"numNATRules", proxier.natRules.Lines(),
	)
	proxier.logger.V(9).Info("Restoring iptables", "rules", proxier.iptablesData.Bytes())

	// 使用 NoFlushTables，避免清空表中非 Kubernetes 创建的链。
	err := proxier.iptables.RestoreAll(proxier.iptablesData.Bytes(), utiliptables.NoFlushTables, utiliptables.RestoreCounters)
	if err != nil {
		if pErr, ok := err.(utiliptables.ParseError); ok {
			lines := utiliptables.ExtractLines(proxier.iptablesData.Bytes(), pErr.Line(), 3)
			proxier.logger.Error(pErr, "Failed to execute iptables-restore", "rules", lines)
		} else {
			proxier.logger.Error(err, "Failed to execute iptables-restore")
		}
		metrics.IPTablesRestoreFailuresTotal.WithLabelValues(string(proxier.ipFamily)).Inc()
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

	// 更新 Service 健康检查。端点列表可能包含非 "OnlyLocal" 的 Service，
	// 但 Service 列表只包含 OnlyLocal 的，serviceHealthServer 会忽略这些端点。
	// 更新 Service 健康检查。端点列表可能包含非 “OnlyLocal” 的 Service，
	// 但 Service 列表只包含 OnlyLocal 的，serviceHealthServer
	// 会忽略这些端点。
	if err := proxier.serviceHealthServer.SyncServices(proxier.svcPortMap.HealthCheckNodePorts()); err != nil {
		proxier.logger.Error(err, "Error syncing healthcheck services")
	}
	if err := proxier.serviceHealthServer.SyncEndpoints(proxier.endpointsMap.LocalReadyEndpoints()); err != nil {
		proxier.logger.Error(err, "Error syncing healthcheck endpoints")
	}

	if endpointUpdateResult.ConntrackCleanupRequired {
		// 完成收尾工作，清理 UDP Service 的过期 conntrack 表项。
		conntrack.CleanStaleEntries(proxier.conntrack, proxier.ipFamily, proxier.svcPortMap, proxier.endpointsMap)
	}
	return
}

// writeServiceToEndpointRules 向策略链写入会话亲和性规则和负载均衡规则：
// 亲和性规则优先，随后按概率随机匹配各端点（概率按剩余端点数递减），最后一条规则保证匹配。
func (proxier *Proxier) writeServiceToEndpointRules(natRules proxyutil.LineBuffer, svcPortNameString string, svcInfo proxy.ServicePort, svcChain utiliptables.Chain, endpoints []proxy.Endpoint, args []string) {
	// 首先写入会话亲和性规则（如适用）。
	if svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP {
		for _, ep := range endpoints {
			epInfo, ok := ep.(*endpointInfo)
			if !ok {
				continue
			}
			comment := fmt.Sprintf(`"%s -> %s"`, svcPortNameString, epInfo.String())

			args = append(args[:0],
				"-A", string(svcChain),
			)
			args = proxier.appendServiceCommentLocked(args, comment)
			args = append(args,
				"-m", "recent", "--name", string(epInfo.ChainName),
				"--rcheck", "--seconds", strconv.Itoa(svcInfo.StickyMaxAgeSeconds()), "--reap",
				"-j", string(epInfo.ChainName),
			)
			natRules.Write(args)
		}
	}

	// 然后写入负载均衡规则。
	numEndpoints := len(endpoints)
	for i, ep := range endpoints {
		epInfo, ok := ep.(*endpointInfo)
		if !ok {
			continue
		}
		comment := fmt.Sprintf(`"%s -> %s"`, svcPortNameString, epInfo.String())

		args = append(args[:0], "-A", string(svcChain))
		args = proxier.appendServiceCommentLocked(args, comment)
		if i < (numEndpoints - 1) {
			// 每条规则都是概率匹配。
			// 概率按 1/n、1/(n-1)… 递减，使剩余端点被等概率选中；
			// 使用预计算的字符串避免每次同步都做浮点数转换。
			// 每条规则都是概率匹配。
			args = append(args,
				"-m", "statistic",
				"--mode", "random",
				"--probability", proxier.probability(numEndpoints-i))
		}
		// 最后一条（或 n == 1 时唯一一条）规则必然匹配，避免概率舍入导致漏选。
		natRules.Write(args, "-j", string(epInfo.ChainName))
	}
}
