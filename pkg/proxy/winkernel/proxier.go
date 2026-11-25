//go:build windows

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

package winkernel

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Microsoft/hnslib"
	"github.com/Microsoft/hnslib/hcn"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	apiutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	"k8s.io/kubernetes/pkg/proxy/metaproxier"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/kubernetes/pkg/proxy/runner"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	netutils "k8s.io/utils/net"
)

// HostMacProvider 用于查询指定节点 IP 对应的宿主机 MAC 地址。
type HostMacProvider interface {
	GetHostMac(nodeIP net.IP) string
}

// externalIPInfo 记录 Service ExternalIP 对应的 HNS 策略 ID。
type externalIPInfo struct {
	ip    string
	hnsID string
}

// String 返回 ExternalIP 及其 HNS 策略 ID 的可读表示，用于日志。
func (info externalIPInfo) String() string {
	return fmt.Sprintf("HnsID:%s, IP:%s", info.hnsID, info.ip)
}

// loadBalancerIngressInfo 记录 LoadBalancer Ingress IP 及健康检查策略的 HNS ID。
type loadBalancerIngressInfo struct {
	ip               string
	hnsID            string
	healthCheckHnsID string
}

// String 返回 Ingress IP 与相关策略 ID 的可读表示，用于日志。
func (info loadBalancerIngressInfo) String() string {
	if len(info.healthCheckHnsID) > 0 {
		return fmt.Sprintf("HealthCheckHnsID:%s, IP:%s", info.healthCheckHnsID, info.ip)
	}
	return fmt.Sprintf("HnsID:%s, IP:%s", info.hnsID, info.ip)
}

// loadBalancerInfo 包装一条 HNS 负载均衡器策略的 ID。
type loadBalancerInfo struct {
	hnsID string
}

// String 返回 HNS 策略 ID，便于在日志中识别。
func (info loadBalancerInfo) String() string {
	return fmt.Sprintf("HnsID:%s", info.hnsID)
}

// loadBalancerIdentifier 唯一标识一条负载均衡器策略：前端配置加后端哈希。
type loadBalancerIdentifier struct {
	protocol      uint16
	internalPort  uint16
	externalPort  uint16
	vip           string
	endpointsHash [20]byte
	isIPv6        bool // isIPv6 保证标识在 IP 协议族间唯一：双栈 etp:local 服务的 NodePort 策略 VIP 为空且端口协议相同，而端点哈希只统计本地端点（同一对象内同时含 v4/v6 IP），因此必须靠该字段区分。
}

// String 返回前端配置的可读表示，用于日志。
func (info loadBalancerIdentifier) String() string {
	return fmt.Sprintf("VIP:%s, Protocol:%d, InternalPort:%d, ExternalPort:%d", info.vip, info.protocol, info.internalPort, info.externalPort)
}

// loadBalancerFlags 是 HNS 策略配置标志的内部抽象，映射到 HCN 标志位。
type loadBalancerFlags struct {
	isILB           bool
	isDSR           bool
	isVipExternalIP bool
	localRoutedVIP  bool
	useMUX          bool
	preserveDIP     bool
	sessionAffinity bool
	isIPv6          bool
}

// serviceInfo 保存服务端口在 Windows 内核代理中需要的扩展信息。
type serviceInfo struct {
	*proxy.BaseServicePortInfo
	targetPort             int
	externalIPs            []*externalIPInfo
	loadBalancerIngressIPs []*loadBalancerIngressInfo
	hnsID                  string
	nodePorthnsID          string
	policyApplied          bool
	remoteEndpoint         *endpointInfo
	hns                    HostNetworkService
	preserveDIP            bool
	localTrafficDSR        bool
	internalTrafficLocal   bool
	winProxyOptimization   bool
}

// String 返回服务的关键 HNS 资源与端口信息，用于日志。
func (info serviceInfo) String() string {
	svcInfoStr := fmt.Sprintf("HnsID:%s, TargetPort:%d", info.hnsID, info.targetPort)
	if info.nodePorthnsID != "" {
		svcInfoStr = fmt.Sprintf("%s, NodePortHnsID:%s", svcInfoStr, info.nodePorthnsID)
	}
	if len(info.externalIPs) > 0 {
		svcInfoStr = fmt.Sprintf("%s, ExternalIPs:%v", svcInfoStr, info.externalIPs)
	}
	if len(info.loadBalancerIngressIPs) > 0 {
		svcInfoStr = fmt.Sprintf("%s, IngressIPs:%v", svcInfoStr, info.loadBalancerIngressIPs)
	}
	return svcInfoStr
}

// hnsNetworkInfo 保存 HNS 网络的标识、类型及远程子网配置。
type hnsNetworkInfo struct {
	name          string
	id            string
	networkType   string
	remoteSubnets []*remoteSubnetInfo
}

// remoteSubnetInfo 描述 Overlay 网络中的一条远程子网路由策略。
type remoteSubnetInfo struct {
	destinationPrefix string
	isolationID       uint16
	providerAddress   string
	drMacAddress      string
}

// 网络类型常量，以及单轮同步可清理的陈旧负载均衡器数量上限。
const (
	NETWORK_TYPE_OVERLAY  = "overlay"
	NETWORK_TYPE_L2BRIDGE = "L2Bridge"
	// MAX_COUNT_STALE_LOADBALANCERS 限制单轮 syncProxyRules 清理的陈旧策略数量；
	// 超出上限的条目留到下一轮处理，避免单次同步耗时过长。
	MAX_COUNT_STALE_LOADBALANCERS = 20
)

// loadBalancerType 标识负载均衡器类型，用于指标标签。
type loadBalancerType string

// 负载均衡器类型常量，用于指标标签。
const (
	// lbTypeClusterIP 表示 ClusterIP 策略。
	lbTypeClusterIP loadBalancerType = "clusterip"
	// lbTypeNodePort 表示 NodePort 策略。
	lbTypeNodePort loadBalancerType = "nodeport"
	// lbTypeExternalIP 表示 ExternalIP 策略。
	lbTypeExternalIP loadBalancerType = "externalip"
	// lbTypeIngressIP 表示 LoadBalancer Ingress IP 策略。
	lbTypeIngressIP loadBalancerType = "ingressip"
	// lbTypeHealthCheck 表示健康检查策略。
	lbTypeHealthCheck loadBalancerType = "healthcheck"
)

// lbErrorType 对负载均衡器操作错误分类，用于指标标签。
type lbErrorType string

// 负载均衡器错误分类常量，用于指标标签。
const (
	// lbErrNetworkNotFound 表示目标 HNS 网络不存在。
	lbErrNetworkNotFound lbErrorType = "network_not_found"
	// lbErrPortAlreadyExists 表示前端端口已被占用。
	lbErrPortAlreadyExists lbErrorType = "port_already_exists"
	// lbErrNotImplemented 表示当前系统版本不支持该操作。
	lbErrNotImplemented lbErrorType = "not_implemented"
	// lbErrInvalidIP 表示传入了非法 IP 地址。
	lbErrInvalidIP lbErrorType = "invalid_ip"
	// lbErrOther 表示未能归类的其他错误。
	lbErrOther lbErrorType = "other"
	// lbErrNone 表示没有错误。
	lbErrNone lbErrorType = "none"
)

// classifyLBError 将 HNS/HCN 错误归入有限类别，避免指标标签基数无限增长。
func classifyLBError(err error) lbErrorType {
	if err == nil {
		// 实际流程中不会走到这里；保留该值只是为了完整性，同时不增加错误类别数量。
		return lbErrNone
	}
	if hcn.IsPortAlreadyExistsError(err) {
		return lbErrPortAlreadyExists
	}
	if hcn.IsNetworkNotFoundError(err) {
		return lbErrNetworkNotFound
	}
	if hcn.IsNotImplemented(err) {
		return lbErrNotImplemented
	}
	if hcn.IsInvalidIPError(err) {
		return lbErrInvalidIP
	}
	return lbErrOther
}

// newHostNetworkService 创建 HNS 服务实现，并返回宿主机支持的 HCN 特性。
func newHostNetworkService(hcnImpl HcnService) (HostNetworkService, hcn.SupportedFeatures) {
	var h HostNetworkService
	supportedFeatures := hcnImpl.GetSupportedFeatures()
	klog.V(3).InfoS("HNS Supported features", "hnsSupportedFeatures", supportedFeatures)
	if supportedFeatures.Api.V2 {
		h = hns{
			hcn: hcnImpl,
		}
	} else {
		panic("Windows HNS Api V2 required. This version of windows does not support API V2")
	}
	return h, supportedFeatures
}

// logFormattedEndpoints 在详细日志级别记录参与端点映射变更的所有端点及其状态，
// 主要用于排障。
func logFormattedEndpoints(logMsg string, logLevel klog.Level, svcPortName proxy.ServicePortName, eps []proxy.Endpoint) {
	if klog.V(logLevel).Enabled() {
		var epInfo string
		for _, v := range eps {
			epInfo = epInfo + fmt.Sprintf("\n  %s={Ready:%v,Serving:%v,Terminating:%v,IsRemote:%v}", v.String(), v.IsReady(), v.IsServing(), v.IsTerminating(), !v.IsLocal())
		}
		klog.V(logLevel).InfoS(logMsg, "svcPortName", svcPortName, "endpoints", epInfo)
	}
}

// cleanupStaleLoadbalancers 清理上一轮删除失败、仍待删除的负载均衡器，
// 相当于对残留策略的自愈重试。
func (proxier *Proxier) cleanupStaleLoadbalancers() {
	i := 0
	countStaleLB := len(proxier.mapStaleLoadbalancers)
	if countStaleLB == 0 {
		return
	}
	klog.V(3).InfoS("Cleanup of stale loadbalancers triggered", "LB Count", countStaleLB)
	for lbID, lbType := range proxier.mapStaleLoadbalancers {
		i++
		if err := proxier.hns.deleteLoadBalancer(lbID); err == nil {
			delete(proxier.mapStaleLoadbalancers, lbID)
		} else {
			metrics.WinKernelLBDeleteFailure.WithLabelValues(string(proxier.ipFamily), string(lbType), string(classifyLBError(err))).Inc()
		}
		if i == MAX_COUNT_STALE_LOADBALANCERS {
			// 本轮已达到清理上限，剩余条目留到下一轮同步。
			break
		}
	}
	countStaleLB = len(proxier.mapStaleLoadbalancers)
	if countStaleLB > 0 {
		klog.V(3).InfoS("Stale loadbalancers still remaining", "LB Count", countStaleLB, "stale_lb_ids", proxier.mapStaleLoadbalancers)
	}
}

// getNetworkName 确定 CNI 网络名：优先使用配置，未配置时回退到 KUBE_NETWORK 环境变量。
func getNetworkName(hnsNetworkName string) (string, error) {
	if len(hnsNetworkName) == 0 {
		klog.V(3).InfoS("Flag --network-name not set, checking environment variable")
		hnsNetworkName = os.Getenv("KUBE_NETWORK")
		if len(hnsNetworkName) == 0 {
			return "", fmt.Errorf("Environment variable KUBE_NETWORK and network-flag not initialized")
		}
	}
	return hnsNetworkName, nil
}

// getNetworkInfo 等待并查询 HNS 网络：CNI 可能尚未创建网络，因此失败时每秒重试。
func getNetworkInfo(hns HostNetworkService, hnsNetworkName string) (*hnsNetworkInfo, error) {
	hnsNetworkInfo, err := hns.getNetworkByName(hnsNetworkName)
	for err != nil {
		klog.ErrorS(err, "Unable to find HNS Network specified, please check network name and CNI deployment", "hnsNetworkName", hnsNetworkName)
		time.Sleep(1 * time.Second)
		hnsNetworkInfo, err = hns.getNetworkByName(hnsNetworkName)
	}
	return hnsNetworkInfo, err
}

// isOverlay 判断 HNS 网络是否为 Overlay 类型。
func isOverlay(hnsNetworkInfo *hnsNetworkInfo) bool {
	return strings.EqualFold(hnsNetworkInfo.networkType, NETWORK_TYPE_OVERLAY)
}

// endpointInfo 保存端点在 HNS 中的资源信息及其就绪状态。
type endpointInfo struct {
	ip              string
	port            uint16
	isLocal         bool
	macAddress      string
	hnsID           string
	refCount        *uint16
	providerAddress string
	hns             HostNetworkService

	// 端点的就绪、服务和终止状态。
	ready       bool
	serving     bool
	terminating bool
}

// String 实现 proxy.Endpoint 接口，用于日志输出。
func (info endpointInfo) String() string {
	return fmt.Sprintf("HnsID:%s, Address:%s", info.hnsID, net.JoinHostPort(info.ip, strconv.Itoa(int(info.port))))
}

// IsLocal 实现 proxy.Endpoint 接口，判断端点是否在本节点。
func (info *endpointInfo) IsLocal() bool {
	return info.isLocal
}

// IsReady 在端点就绪且不在终止中时返回 true。
func (info *endpointInfo) IsReady() bool {
	return info.ready
}

// IsServing 只要端点处于服务状态就返回 true，不关心其是否正在终止。
func (info *endpointInfo) IsServing() bool {
	return info.serving
}

// IsTerminating 在端点正在终止时返回 true。
func (info *endpointInfo) IsTerminating() bool {
	return info.terminating
}

// ZoneHints 返回端点的可用区提示；Winkernel 当前不提供该信息。
func (info *endpointInfo) ZoneHints() sets.Set[string] {
	return sets.Set[string]{}
}

// NodeHints 返回端点的节点提示；Winkernel 当前不提供该信息。
func (info *endpointInfo) NodeHints() sets.Set[string] {
	return sets.Set[string]{}
}

// IP 返回端点的 IP 部分，是 proxy.Endpoint 接口的一部分。
func (info *endpointInfo) IP() string {
	return info.ip
}

// Port 返回端点的端口部分。
func (info *endpointInfo) Port() int {
	return int(info.port)
}

// conjureMac 根据 MAC 前缀和 IP 地址生成 MAC，以保证负载均衡时各端点地址不同。
// 理论上仍可能碰撞，但该 MAC 仅用于远端端点标识，不会发送到网络上。
func conjureMac(macPrefix string, ip net.IP) string {
	if ip4 := ip.To4(); ip4 != nil {
		a, b, c, d := ip4[0], ip4[1], ip4[2], ip4[3]
		return fmt.Sprintf("%v-%02x-%02x-%02x-%02x", macPrefix, a, b, c, d)
	} else if ip6 := ip.To16(); ip6 != nil {
		a, b, c, d := ip6[15], ip6[14], ip6[13], ip6[12]
		return fmt.Sprintf("%v-%02x-%02x-%02x-%02x", macPrefix, a, b, c, d)
	}
	return "02-11-22-33-44-55"
}

// updateTerminatedEndpoints 通过“加入旧映射中的端点、删除新映射中的端点”来记录已终止端点，
// 最终留下的就是只存在于旧映射里的那些。
func (proxier *Proxier) updateTerminatedEndpoints(eps []proxy.Endpoint, isOldEndpointsMap bool) {
	for _, ep := range eps {
		if !ep.IsLocal() {
			if isOldEndpointsMap {
				proxier.terminatedEndpoints[ep.IP()] = true
			} else {
				delete(proxier.terminatedEndpoints, ep.IP())
			}
		}
	}
}

// endpointsMapChange 比较新旧端点映射并触发对应的策略清理。
func (proxier *Proxier) endpointsMapChange(oldEndpointsMap, newEndpointsMap proxy.EndpointsMap) {
	// 记录新旧映射涉及的服务端口，便于按优化注解跳过重复清理。
	var svcPortMap = make(map[proxy.ServicePortName]bool)

	var logLevel klog.Level = 5
	for svcPortName, eps := range oldEndpointsMap {
		logFormattedEndpoints("endpointsMapChange oldEndpointsMap", logLevel, svcPortName, eps)
		svcPortMap[svcPortName] = true
		proxier.updateTerminatedEndpoints(eps, true)
		proxier.onEndpointsMapChange(&svcPortName, false)
	}

	for svcPortName, eps := range newEndpointsMap {
		logFormattedEndpoints("endpointsMapChange newEndpointsMap", logLevel, svcPortName, eps)
		// redundantCleanup 表示该服务端口已在旧映射的清理流程中处理过一次。
		proxier.updateTerminatedEndpoints(eps, false)
		redundantCleanup := svcPortMap[svcPortName]
		proxier.onEndpointsMapChange(&svcPortName, redundantCleanup)
	}
}

// onEndpointsMapChange 在端点映射变化时清理受影响服务的 HNS 策略与端点引用。
func (proxier *Proxier) onEndpointsMapChange(svcPortName *proxy.ServicePortName, redundantCleanup bool) {

	svc, exists := proxier.svcPortMap[*svcPortName]

	if exists {
		svcInfo, ok := svc.(*serviceInfo)

		if !ok {
			klog.ErrorS(nil, "Failed to cast serviceInfo", "servicePortName", svcPortName)
			return
		}

		if svcInfo.winProxyOptimization && redundantCleanup {
			// 开启 winProxyOptimization 时忽略对同一服务端口的第二次清理，
			// 避免重复递减引用计数。
			return
		}

		klog.V(3).InfoS("Endpoints are modified. Service is stale", "servicePortName", svcPortName)
		svcInfo.cleanupAllPolicies(proxier.endpointsMap[*svcPortName], proxier.mapStaleLoadbalancers, true, string(proxier.ipFamily))
	} else {
		// 服务已被删除时不再处理负载均衡器，只清理远端端点。
		klog.V(3).InfoS("Endpoints are orphaned, cleaning up")
		// 清理端点引用计数。
		epInfos, exists := proxier.endpointsMap[*svcPortName]

		if exists {
			// 清理端点引用计数。
			for _, ep := range epInfos {
				epInfo, ok := ep.(*endpointInfo)

				if ok {
					epInfo.Cleanup()
				}

			}
		}
	}
}

// serviceMapChange 对新旧服务映射中的服务端口触发策略清理。
func (proxier *Proxier) serviceMapChange(previous, current proxy.ServicePortMap) {
	for svcPortName := range current {
		proxier.onServiceMapChange(&svcPortName)
	}

	for svcPortName := range previous {
		if _, ok := current[svcPortName]; ok {
			continue
		}
		proxier.onServiceMapChange(&svcPortName)
	}
}

// onServiceMapChange 在服务映射变化时清理该服务端口的 HNS 策略。
func (proxier *Proxier) onServiceMapChange(svcPortName *proxy.ServicePortName) {

	svc, exists := proxier.svcPortMap[*svcPortName]

	if exists {
		svcInfo, ok := svc.(*serviceInfo)

		if !ok {
			klog.ErrorS(nil, "Failed to cast serviceInfo", "servicePortName", svcPortName)
			return
		}

		klog.V(3).InfoS("Updating existing service port", "servicePortName", svcPortName, "clusterIP", svcInfo.ClusterIP(), "port", svcInfo.Port(), "protocol", svcInfo.Protocol())
		svcInfo.cleanupAllPolicies(proxier.endpointsMap[*svcPortName], proxier.mapStaleLoadbalancers, false, string(proxier.ipFamily))
	}
}

// newEndpointInfo 创建包装 endpointInfo 的 proxy.Endpoint 实现。
func (proxier *Proxier) newEndpointInfo(baseInfo *proxy.BaseEndpointInfo, _ *proxy.ServicePortName) proxy.Endpoint {

	info := &endpointInfo{
		ip:         baseInfo.IP(),
		port:       uint16(baseInfo.Port()),
		isLocal:    baseInfo.IsLocal(),
		macAddress: conjureMac("02-11", netutils.ParseIPSloppy(baseInfo.IP())),
		refCount:   new(uint16),
		hnsID:      "",
		hns:        proxier.hns,

		ready:       baseInfo.IsReady(),
		serving:     baseInfo.IsServing(),
		terminating: baseInfo.IsTerminating(),
	}

	return info
}

// newSourceVIP 在 Overlay 网络中创建代表 SourceVIP 的本地端点。
func newSourceVIP(hns HostNetworkService, network string, ip string, mac string, providerAddress string) (*endpointInfo, error) {
	hnsEndpoint := &endpointInfo{
		ip:              ip,
		isLocal:         true,
		macAddress:      mac,
		providerAddress: providerAddress,

		ready:       true,
		serving:     true,
		terminating: false,
	}
	ep, err := hns.createEndpoint(hnsEndpoint, network)
	return ep, err
}

// DecrementRefCount 递减远端端点的引用计数，但不触发实际删除。
func (ep *endpointInfo) DecrementRefCount() {
	klog.V(3).InfoS("Decrementing Endpoint RefCount", "endpointInfo", ep)
	if !ep.IsLocal() && ep.refCount != nil && *ep.refCount > 0 {
		*ep.refCount--
	}
	refCount := 0
	if ep.refCount != nil {
		refCount = int(*ep.refCount)
	}
	klog.V(5).InfoS("Endpoint RefCount after decrement.", "endpointInfo", ep, "refCount", refCount)
}

// Cleanup 释放端点引用；引用归零时删除对应的远端 HNS 端点。
func (ep *endpointInfo) Cleanup() {
	klog.V(3).InfoS("Endpoint cleanup", "endpointInfo", ep)
	if !ep.IsLocal() && ep.refCount != nil {
		*ep.refCount--

		// 仅在没有任何服务引用时删除远端 HNS 端点；
		// 本地端点由其他组件创建，本代理绝不能删除，只清理自己创建的远端端点。
		if *ep.refCount <= 0 && !ep.IsLocal() {
			klog.V(4).InfoS("Removing endpoints, since no one is referencing it", "endpoint", ep)
			err := ep.hns.deleteEndpoint(ep.hnsID)
			if err == nil {
				ep.hnsID = ""
			} else {
				klog.ErrorS(err, "Endpoint deletion failed", "ip", ep.IP())
			}
		}

		ep.refCount = nil
	}
}

// getRefCount 返回端点 ID 对应的共享引用计数，不存在时创建。
func (refCountMap endPointsReferenceCountMap) getRefCount(hnsID string) *uint16 {
	refCount, exists := refCountMap[hnsID]
	if !exists {
		refCountMap[hnsID] = new(uint16)
		refCount = refCountMap[hnsID]
	}
	return refCount
}

// newServiceInfo 创建包装 serviceInfo 的 proxy.ServicePort 实现。
func (proxier *Proxier) newServiceInfo(port *v1.ServicePort, service *v1.Service, bsvcPortInfo *proxy.BaseServicePortInfo) proxy.ServicePort {
	info := &serviceInfo{BaseServicePortInfo: bsvcPortInfo}
	preserveDIP := service.Annotations["preserve-destination"] == "true"
	// 该注解用于开启优化：端点变化时只递减引用计数而不删除负载均衡器。
	winProxyOptimization := !(strings.ToUpper(service.Annotations["winProxyOptimization"]) == "DISABLED")
	localTrafficDSR := service.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyLocal
	var internalTrafficLocal bool
	if service.Spec.InternalTrafficPolicy != nil {
		internalTrafficLocal = *service.Spec.InternalTrafficPolicy == v1.ServiceInternalTrafficPolicyLocal
	}
	hcnImpl := proxier.hcn
	err := hcnImpl.DsrSupported()
	if err != nil {
		preserveDIP = false
		localTrafficDSR = false
	}
	// TargetPort 以名称指定时此处为 0，真实端口稍后从端点获取。
	targetPort := 0
	if port.TargetPort.Type == intstr.Int {
		targetPort = port.TargetPort.IntValue()
	}

	info.preserveDIP = preserveDIP
	info.targetPort = targetPort
	info.hns = proxier.hns
	info.localTrafficDSR = localTrafficDSR
	info.internalTrafficLocal = internalTrafficLocal
	info.winProxyOptimization = winProxyOptimization
	klog.V(3).InfoS("Flags enabled for service", "service", service.Name, "localTrafficDSR", localTrafficDSR, "internalTrafficLocal", internalTrafficLocal, "preserveDIP", preserveDIP, "winProxyOptimization", winProxyOptimization)

	for _, eip := range info.ExternalIPs() {
		info.externalIPs = append(info.externalIPs, &externalIPInfo{ip: eip.String()})
	}

	for _, ingress := range info.LoadBalancerVIPs() {
		info.loadBalancerIngressIPs = append(info.loadBalancerIngressIPs, &loadBalancerIngressInfo{ip: ingress.String()})
	}

	return info
}

// findRemoteSubnetProviderAddress 查找 IP 所属远程子网的 ProviderAddress。
func (network hnsNetworkInfo) findRemoteSubnetProviderAddress(ip string) string {
	var providerAddress string
	for _, rs := range network.remoteSubnets {
		_, ipNet, err := netutils.ParseCIDRSloppy(rs.destinationPrefix)
		if err != nil {
			klog.ErrorS(err, "Failed to parse CIDR")
		}
		if ipNet.Contains(netutils.ParseIPSloppy(ip)) {
			providerAddress = rs.providerAddress
		}
		if ip == rs.providerAddress {
			providerAddress = rs.providerAddress
		}
	}

	return providerAddress
}

// endPointsReferenceCountMap 按 HNS 端点 ID 记录远端端点的引用计数。
type endPointsReferenceCountMap map[string]*uint16

// Proxier 是基于 HNS 的单栈内核代理实现。
type Proxier struct {
	// ipFamily 是本实例负责的 IP 协议族。
	ipFamily v1.IPFamily

	// endpointsChanges 与 serviceChanges 保存上次同步策略以来端点/服务的所有变更；
	// 单个对象的变更会被累积：previous 是全部变更前的状态，current 是应用后的状态。
	endpointsChanges  *proxy.EndpointsChangeTracker
	serviceChanges    *proxy.ServiceChangeTracker
	endPointsRefCount endPointsReferenceCountMap
	mu                sync.Mutex // 保护以下字段
	svcPortMap        proxy.ServicePortMap
	endpointsMap      proxy.EndpointsMap
	// endpointSlicesSynced 与 servicesSynced 在启动后首次同步完成时置为 true，
	// 用于避免 kube-proxy 重启后用不完整的数据更新 HNS 策略。
	endpointSlicesSynced bool
	servicesSynced       bool
	initialized          int32
	syncRunner           *runner.BoundedFrequencyRunner // 控制对 syncProxyRules 的调用
	// 以下字段实际只读，因此不需要持有 mu。
	nodeName string
	nodeIP   net.IP

	serviceHealthServer healthcheck.ServiceHealthServer
	healthzServer       *healthcheck.ProxyHealthServer

	hns               HostNetworkService
	hcn               HcnService
	network           hnsNetworkInfo
	sourceVip         string
	hostMac           string
	isDSR             bool
	supportedFeatures hcn.SupportedFeatures
	healthzPort       int

	forwardHealthCheckVip bool
	rootHnsEndpointName   string
	mapStaleLoadbalancers map[string]loadBalancerType // 记录上一轮删除失败、等待重试的陈旧策略，值为策略类型。
	terminatedEndpoints   map[string]bool             // 记录已终止的端点，键为 IP:端口。
}

// localPort 描述一个本地监听端口，仅用于错误提示。
type localPort struct {
	desc     string
	ip       string
	port     int
	protocol string
}

// String 返回本地端口的可读描述。
func (lp *localPort) String() string {
	return fmt.Sprintf("%q (%s:%d/%s)", lp.desc, lp.ip, lp.port, lp.protocol)
}

// Enum 将 Kubernetes 协议名映射为 HCN 使用的 IANA 协议号。
func Enum(p v1.Protocol) uint16 {
	if p == v1.ProtocolTCP {
		return 6
	}
	if p == v1.ProtocolUDP {
		return 17
	}
	if p == v1.ProtocolSCTP {
		return 132
	}
	return 0
}

// closeable 抽象具备 Close 方法的资源。
type closeable interface {
	Close() error
}

// Proxier 实现了 proxy.Provider 接口。
var _ proxy.Provider = &Proxier{}

// NewProxier 创建一个新的单栈 winkernel 代理。
func NewProxier(
	config *kubeproxyconfig.KubeProxyConfiguration,
	ipFamily v1.IPFamily,
	nodeName string,
	nodeIP net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
) (*Proxier, error) {
	// Windows 上监听所有节点地址，因此地址列表传入 nil。
	nodePortAddresses := proxyutil.NewNodePortAddresses(ipFamily, nil)
	serviceHealthServer := healthcheck.NewServiceHealthServer(nodeName, recorder, nodePortAddresses, healthzServer, ipFamily)

	var healthzPort int
	if len(config.HealthzBindAddress) > 0 {
		_, port, _ := net.SplitHostPort(config.HealthzBindAddress)
		healthzPort, _ = strconv.Atoi(port)
	}

	hcnImpl := newHcnImpl()
	proxier, err := newProxierInternal(
		ipFamily,
		nodeName,
		nodeIP,
		serviceHealthServer,
		healthzServer,
		healthzPort,
		hcnImpl,
		&localHostMacProvider{},
		config.Winkernel,
		true, // waitForHNSOverlay
	)
	if err != nil {
		return nil, err
	}

	syncPeriod := config.SyncPeriod.Duration
	minSyncPeriod := config.MinSyncPeriod.Duration

	klog.V(3).Info("Record sync params", "minSyncPeriod", minSyncPeriod, "syncPeriod", syncPeriod, "maxSyncPeriod", syncPeriod)
	proxier.syncRunner = runner.NewBoundedFrequencyRunner("sync-runner", proxier.syncProxyRules, minSyncPeriod, syncPeriod, syncPeriod)

	return proxier, nil
}

// newProxierInternal 允许在内部测试中注入模拟的 HCN 实现。
func newProxierInternal(
	ipFamily v1.IPFamily,
	nodeName string,
	nodeIP net.IP,
	serviceHealthServer healthcheck.ServiceHealthServer,
	healthzServer *healthcheck.ProxyHealthServer,
	healthzPort int,
	hcnImpl HcnService,
	hostMacProvider HostMacProvider,
	config kubeproxyconfig.KubeProxyWinkernelConfiguration,
	waitForHNSOverlay bool,
) (*Proxier, error) {
	hns, supportedFeatures := newHostNetworkService(hcnImpl)
	hnsNetworkName, err := getNetworkName(config.NetworkName)
	if err != nil {
		return nil, err
	}

	klog.V(3).InfoS("Cleaning up old HNS policy lists")
	hcnImpl.DeleteAllHnsLoadBalancerPolicy()

	// 查询 HNS 网络信息。
	hnsNetworkInfo, err := getNetworkInfo(hns, hnsNetworkName)
	if err != nil {
		return nil, err
	}

	// 首次查询可能早于远程子网策略下发或管理 IP 更新；
	// 等待片刻后重新查询，以拿到完整的网络信息。
	if isOverlay(hnsNetworkInfo) {
		if waitForHNSOverlay {
			time.Sleep(10 * time.Second)
		}

		hnsNetworkInfo, err = hns.getNetworkByName(hnsNetworkName)
		if err != nil {
			return nil, fmt.Errorf("could not find HNS network %s", hnsNetworkName)
		}
	}

	klog.V(1).InfoS("Hns Network loaded", "hnsNetworkInfo", hnsNetworkInfo)
	isDSR := config.EnableDSR
	if isDSR && !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WinDSR) {
		return nil, fmt.Errorf("WinDSR feature gate not enabled")
	}

	err = hcnImpl.DsrSupported()
	if isDSR && err != nil {
		return nil, err
	}

	var sourceVip string
	var hostMac string
	if isOverlay(hnsNetworkInfo) {
		if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WinOverlay) {
			return nil, fmt.Errorf("WinOverlay feature gate not enabled")
		}
		err = hcnImpl.RemoteSubnetSupported()
		if err != nil {
			return nil, err
		}
		sourceVip = config.SourceVip
		if len(sourceVip) == 0 {
			return nil, fmt.Errorf("source-vip flag not set")
		}

		if nodeIP.IsUnspecified() {
			// 节点 IP 未指定时尝试解析出实际可用的地址。
			klog.V(2).InfoS("Node ip was unspecified, attempting to find node ip")
			nodeIP, err = apiutil.ResolveBindAddress(nodeIP)
			if err != nil {
				klog.InfoS("Failed to find an ip. You may need set the --bind-address flag", "err", err)
			}
		}

		hostMac = hostMacProvider.GetHostMac(nodeIP)

		if len(hostMac) == 0 {
			return nil, fmt.Errorf("could not find host mac address for %s", nodeIP)
		}
	}

	proxier := &Proxier{
		ipFamily:              ipFamily,
		endPointsRefCount:     make(endPointsReferenceCountMap),
		svcPortMap:            make(proxy.ServicePortMap),
		endpointsMap:          make(proxy.EndpointsMap),
		nodeName:              nodeName,
		nodeIP:                nodeIP,
		serviceHealthServer:   serviceHealthServer,
		healthzServer:         healthzServer,
		hns:                   hns,
		hcn:                   hcnImpl,
		network:               *hnsNetworkInfo,
		sourceVip:             sourceVip,
		hostMac:               hostMac,
		isDSR:                 isDSR,
		supportedFeatures:     supportedFeatures,
		healthzPort:           healthzPort,
		rootHnsEndpointName:   config.RootHnsEndpointName,
		forwardHealthCheckVip: config.ForwardHealthCheckVip,
		mapStaleLoadbalancers: make(map[string]loadBalancerType),
		terminatedEndpoints:   make(map[string]bool),
	}

	serviceChanges := proxy.NewServiceChangeTracker(ipFamily, proxier.newServiceInfo, proxier.serviceMapChange)
	endPointChangeTracker := proxy.NewEndpointsChangeTracker(ipFamily, nodeName, proxier.newEndpointInfo, proxier.endpointsMapChange)
	proxier.endpointsChanges = endPointChangeTracker
	proxier.serviceChanges = serviceChanges

	return proxier, nil
}

// NewDualStackProxier 分别为 IPv4 和 IPv6 创建单栈代理，并用 meta-proxier 组合。
func NewDualStackProxier(
	config *kubeproxyconfig.KubeProxyConfiguration,
	nodeName string,
	nodeIPs map[v1.IPFamily]net.IP,
	recorder events.EventRecorder,
	healthzServer *healthcheck.ProxyHealthServer,
) (proxy.Provider, error) {

	// 分别创建 IPv4 和 IPv6 两个单栈代理实例。
	ipv4Proxier, err := NewProxier(config, v1.IPv4Protocol,
		nodeName, nodeIPs[v1.IPv4Protocol], recorder, healthzServer)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv4 proxier: %v, nodeName: %s, nodeIP:%v", err, nodeName, nodeIPs[v1.IPv4Protocol])
	}

	ipv6Proxier, err := NewProxier(config, v1.IPv6Protocol,
		nodeName, nodeIPs[v1.IPv6Protocol], recorder, healthzServer)
	if err != nil {
		return nil, fmt.Errorf("unable to create ipv6 proxier: %v, nodeName: %s, nodeIP:%v", err, nodeName, nodeIPs[v1.IPv6Protocol])
	}

	// 返回 meta-proxier，将调用分发到两个单栈实例。
	return metaproxier.NewMetaProxier(ipv4Proxier, ipv6Proxier), nil
}

// CleanupLeftovers 删除代理创建的全部 HNS 规则；遇到错误时记录日志并返回 true。
func CleanupLeftovers() (encounteredError bool) {
	// 删除所有 HNS 负载均衡器策略。
	newHcnImpl().DeleteAllHnsLoadBalancerPolicy()
	// TODO: 删除所有 HNS 远端端点。

	return encounteredError
}

// cleanupAllPolicies 清理服务关联的全部 HNS 策略与端点引用。
func (svcInfo *serviceInfo) cleanupAllPolicies(endpoints []proxy.Endpoint, mapStaleLoadbalancers map[string]loadBalancerType, isEndpointChange bool, ipFamilyStr string) {
	klog.V(3).InfoS("Service cleanup", "serviceInfo", svcInfo)
	// 开启优化且仅端点变化时跳过策略与远端端点删除，只递减引用计数。
	winProxyOptimization := isEndpointChange && svcInfo.winProxyOptimization
	if winProxyOptimization {
		klog.V(3).InfoS("Skipped loadbalancer deletion.", "hnsID", svcInfo.hnsID, "nodePorthnsID", svcInfo.nodePorthnsID, "winProxyOptimization", svcInfo.winProxyOptimization, "isEndpointChange", isEndpointChange)
	} else {
		// 此处不受 policyApplied 限制，需要移除该服务的所有策略。
		svcInfo.deleteLoadBalancerPolicy(mapStaleLoadbalancers, ipFamilyStr)
	}
	// 清理端点引用计数。
	for _, ep := range endpoints {
		epInfo, ok := ep.(*endpointInfo)
		if ok {
			if winProxyOptimization {
				epInfo.DecrementRefCount()
			} else {
				epInfo.Cleanup()
			}
		}
	}
	if svcInfo.remoteEndpoint != nil {
		svcInfo.remoteEndpoint.Cleanup()
	}

	svcInfo.policyApplied = false
}

// deleteLoadBalancerPolicy 删除服务各类 VIP 对应的策略，失败项记入陈旧列表。
func (svcInfo *serviceInfo) deleteLoadBalancerPolicy(mapStaleLoadbalancer map[string]loadBalancerType, ipFamilyStr string) {
	// 删除该服务对应的 HNS 策略；删除失败的记录到陈旧列表以便后续重试。
	hns := svcInfo.hns
	if svcInfo.hnsID != "" {
		klog.V(3).InfoS("Deleting ClusterIP LB during cleanup", "clusterIPLBID", svcInfo.hnsID, "ClusterIP", svcInfo.ClusterIP())
		if err := hns.deleteLoadBalancer(svcInfo.hnsID); err != nil {
			mapStaleLoadbalancer[svcInfo.hnsID] = lbTypeClusterIP
			klog.V(1).ErrorS(err, "Error deleting ClusterIP LB during cleanup.", "hnsID", svcInfo.hnsID, "ClusterIP", svcInfo.ClusterIP())
			metrics.WinKernelLBDeleteFailure.WithLabelValues(ipFamilyStr, string(lbTypeClusterIP), string(classifyLBError(err))).Inc()
		} else {
			// 删除成功后清空 ID，避免后续重复删除。
			svcInfo.hnsID = ""
		}
	}

	if svcInfo.nodePorthnsID != "" {
		klog.V(3).InfoS("Deleting NodePort LB during cleanup", "nodePortLBID", svcInfo.nodePorthnsID, "NodePort", svcInfo.NodePort())
		if err := hns.deleteLoadBalancer(svcInfo.nodePorthnsID); err != nil {
			mapStaleLoadbalancer[svcInfo.nodePorthnsID] = lbTypeNodePort
			klog.V(1).ErrorS(err, "Error deleting NodePort LB during cleanup.", "hnsID", svcInfo.nodePorthnsID, "NodePort", svcInfo.NodePort())
			metrics.WinKernelLBDeleteFailure.WithLabelValues(ipFamilyStr, string(lbTypeNodePort), string(classifyLBError(err))).Inc()
		} else {
			// 删除成功后清空 ID，避免后续重复删除。
			svcInfo.nodePorthnsID = ""
		}
	}

	for _, externalIP := range svcInfo.externalIPs {
		if externalIP.hnsID == "" {
			continue
		}
		klog.V(3).InfoS("Deleting ExternalIP LB during cleanup", "externalIPLBID", externalIP.hnsID, "ExternalIP", externalIP.ip)
		if err := hns.deleteLoadBalancer(externalIP.hnsID); err != nil {
			mapStaleLoadbalancer[externalIP.hnsID] = lbTypeExternalIP
			klog.V(1).ErrorS(err, "Error deleting ExternalIP LB during cleanup.", "hnsID", externalIP.hnsID, "ExternalIP", externalIP.ip)
			metrics.WinKernelLBDeleteFailure.WithLabelValues(ipFamilyStr, string(lbTypeExternalIP), string(classifyLBError(err))).Inc()
		} else {
			// 删除成功后清空 ID，避免后续重复删除。
			externalIP.hnsID = ""
		}
	}
	for _, lbIngressIP := range svcInfo.loadBalancerIngressIPs {
		if lbIngressIP.hnsID != "" {
			klog.V(3).InfoS("Deleting Ingress LB during cleanup", "ingressLBID", lbIngressIP.hnsID, "IngressIP", lbIngressIP.ip)
			if err := hns.deleteLoadBalancer(lbIngressIP.hnsID); err != nil {
				mapStaleLoadbalancer[lbIngressIP.hnsID] = lbTypeIngressIP
				klog.V(1).ErrorS(err, "Error deleting Ingress LB during cleanup.", "hnsID", lbIngressIP.hnsID, "IngressIP", lbIngressIP.ip)
				metrics.WinKernelLBDeleteFailure.WithLabelValues(ipFamilyStr, string(lbTypeIngressIP), string(classifyLBError(err))).Inc()
			} else {
				// 删除成功后清空 ID，避免后续重复删除。
				lbIngressIP.hnsID = ""
			}
		}

		if lbIngressIP.healthCheckHnsID != "" {
			klog.V(3).InfoS("Deleting Ingress LB HealthCheck during cleanup", "ingressLBHealthCheckID", lbIngressIP.healthCheckHnsID, "IngressIP", lbIngressIP.ip)
			if err := hns.deleteLoadBalancer(lbIngressIP.healthCheckHnsID); err != nil {
				mapStaleLoadbalancer[lbIngressIP.healthCheckHnsID] = lbTypeHealthCheck
				klog.V(1).ErrorS(err, "Error deleting Ingress LB HealthCheck during cleanup.", "hnsID", lbIngressIP.healthCheckHnsID, "IngressIP", lbIngressIP.ip)
				metrics.WinKernelLBDeleteFailure.WithLabelValues(ipFamilyStr, string(lbTypeHealthCheck), string(classifyLBError(err))).Inc()
			} else {
				// 删除成功后清空 ID，避免后续重复删除。
				lbIngressIP.healthCheckHnsID = ""
			}
		}
	}
}

// Sync 尽快将代理状态同步到 HNS。
func (proxier *Proxier) Sync() {
	if proxier.healthzServer != nil {
		proxier.healthzServer.QueuedUpdate(proxier.ipFamily)
	}
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Run()
}

// SyncLoop 周期性地触发同步，通常以 goroutine 或主循环方式运行，不会返回。
func (proxier *Proxier) SyncLoop() {
	// 启动时先更新时间戳，防止 Sync() 从未成功时健康检查误判。
	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}
	// informer 仍在同步期间，模拟一个“最后一次变更入队”的时间。
	metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
	proxier.syncRunner.Loop(wait.NeverStop)
}

// setInitialized 以原子方式设置代理的初始化状态。
func (proxier *Proxier) setInitialized(value bool) {
	var initialized int32
	if value {
		initialized = 1
	}
	atomic.StoreInt32(&proxier.initialized, initialized)
}

// isInitialized 判断 Service 与 Endpoint 是否都已完成首次同步。
func (proxier *Proxier) isInitialized() bool {
	return atomic.LoadInt32(&proxier.initialized) > 0
}

// OnServiceAdd 在观测到新 Service 对象创建时调用。
func (proxier *Proxier) OnServiceAdd(service *v1.Service) {
	proxier.OnServiceUpdate(nil, service)
}

// OnServiceUpdate 在观测到已有 Service 对象被修改时调用。
func (proxier *Proxier) OnServiceUpdate(oldService, service *v1.Service) {
	if proxier.serviceChanges.Update(oldService, service) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnServiceDelete 在观测到已有 Service 对象被删除时调用。
func (proxier *Proxier) OnServiceDelete(service *v1.Service) {
	proxier.OnServiceUpdate(service, nil)
}

// OnServiceSynced 在初始事件处理完成、状态完全写入本地缓存后调用一次。
func (proxier *Proxier) OnServiceSynced() {
	proxier.mu.Lock()
	proxier.servicesSynced = true
	proxier.setInitialized(proxier.endpointSlicesSynced)
	proxier.mu.Unlock()

	// 该回调生命周期内只调用一次，因此无条件执行一次同步。
	proxier.syncProxyRules()
}

// OnEndpointSliceAdd 在观测到新 EndpointSlice 对象创建时调用。
func (proxier *Proxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSliceUpdate 在观测到已有 EndpointSlice 对象被修改时调用。
func (proxier *Proxier) OnEndpointSliceUpdate(_, endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSliceDelete 在观测到已有 EndpointSlice 对象被删除时调用。
func (proxier *Proxier) OnEndpointSliceDelete(endpointSlice *discovery.EndpointSlice) {
	if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, true) && proxier.isInitialized() {
		proxier.Sync()
	}
}

// OnEndpointSlicesSynced 在初始事件处理完成、状态完全写入本地缓存后调用一次。
func (proxier *Proxier) OnEndpointSlicesSynced() {
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.setInitialized(proxier.servicesSynced)
	proxier.mu.Unlock()

	// 该回调生命周期内只调用一次，因此无条件执行一次同步。
	proxier.syncProxyRules()
}

// OnServiceCIDRsChanged 在 ServiceCIDR 发生变化时调用，参数提供完整的 CIDR 列表。
func (proxier *Proxier) OnServiceCIDRsChanged(_ []string) {}

// TODO(imroc): 为 winkernel 代理实现 OnTopologyChanged。
// OnTopologyChange 在节点拓扑标签发生变化时调用。
// informer 被调整为只监听运行当前 kube-proxy 的节点更新，
// 以保证收到的标签变化来自本节点。
func (proxier *Proxier) OnTopologyChange(topologyLabels map[string]string) {}

// cleanupAllPolicies 清理所有服务端口的 HNS 策略。
func (proxier *Proxier) cleanupAllPolicies() {
	for svcName, svc := range proxier.svcPortMap {
		svcInfo, ok := svc.(*serviceInfo)
		if !ok {
			klog.ErrorS(nil, "Failed to cast serviceInfo", "serviceName", svcName)
			continue
		}
		svcInfo.cleanupAllPolicies(proxier.endpointsMap[svcName], proxier.mapStaleLoadbalancers, false, string(proxier.ipFamily))
	}
}

// isNetworkNotFoundError 判断错误是否表示 HNS 网络不存在，兼容新旧 hnslib 错误类型。
func isNetworkNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(hcn.NetworkNotFoundError); ok {
		return true
	}
	if _, ok := err.(hnslib.NetworkNotFoundError); ok {
		return true
	}
	return false
}

// isAllEndpointsTerminating 在所有端点都处于终止状态时返回 true；
// 只要有一个端点不在终止中就返回 false。
func (proxier *Proxier) isAllEndpointsTerminating(svcName proxy.ServicePortName, isLocalTrafficDSR bool) bool {
	for _, epInfo := range proxier.endpointsMap[svcName] {
		ep, ok := epInfo.(*endpointInfo)
		if !ok {
			continue
		}
		if isLocalTrafficDSR && !ep.IsLocal() {
			// KEP-1669：ExternalTrafficPolicy 为 Local（DSR 模式）时忽略远端端点。
			continue
		}
		// 就绪探针失败但 Pod 未被删除时，端点状态为 Ready:False、Serving:False、Terminating:False，
		// 这种情况不应视为正在终止。
		if !ep.IsReady() && !ep.IsTerminating() {
			// 未就绪且未终止，忽略。
			continue
		}
		if !ep.IsTerminating() {
			return false
		}
	}
	return true
}

// isAllEndpointsNonServing 在所有端点都不在服务状态时返回 true；
// 只要有一个端点可服务就返回 false。
func (proxier *Proxier) isAllEndpointsNonServing(svcName proxy.ServicePortName, isLocalTrafficDSR bool) bool {
	for _, epInfo := range proxier.endpointsMap[svcName] {
		ep, ok := epInfo.(*endpointInfo)
		if !ok {
			continue
		}
		if isLocalTrafficDSR && !ep.IsLocal() {
			continue
		}
		if ep.IsServing() {
			return false
		}
	}
	return true
}

// updateQueriedEndpoints 将新建端点按 ID 和 IP 写入查询缓存。
func updateQueriedEndpoints(newHnsEndpoint *endpointInfo, queriedEndpoints map[string]*endpointInfo) {
	// 缓存新端点，供本轮同步内复用。
	queriedEndpoints[newHnsEndpoint.hnsID] = newHnsEndpoint
	queriedEndpoints[newHnsEndpoint.ip] = newHnsEndpoint
}

// requiresUpdateLoadbalancer 判断能否原地更新已有策略：需要系统支持且已有策略与后端。
func (proxier *Proxier) requiresUpdateLoadbalancer(lbHnsID string, endpointCount int) bool {
	return proxier.supportedFeatures.ModifyLoadbalancer && lbHnsID != "" && endpointCount > 0
}

// handleUpdateLoadbalancerFailure 处理策略更新失败：若系统不支持该操作则关闭相关特性并继续；
// 返回 true 表示应跳过当前服务的本轮同步。
func (proxier *Proxier) handleUpdateLoadbalancerFailure(err error, hnsID, svcIP string, endpointCount int) (skipIteration bool) {
	if err != nil {
		if hcn.IsNotImplemented(err) {
			klog.Warning("Update loadbalancer policies is not implemented.", "hnsID", hnsID, "svcIP", svcIP, "endpointCount", endpointCount)
			proxier.supportedFeatures.ModifyLoadbalancer = false
		} else {
			klog.ErrorS(err, "Update loadbalancer policy failed", "hnsID", hnsID, "svcIP", svcIP, "endpointCount", endpointCount)
			skipIteration = true
		}
	}
	return skipIteration
}

// syncProxyRules 完成所有 HNS 规则的保存/恢复操作。
// 调用方需持有 proxier.mu（函数内部自行加锁）。
func (proxier *Proxier) syncProxyRules() (retryError error) {
	proxier.mu.Lock()
	defer proxier.mu.Unlock()

	// Service 与 Endpoint 都同步完成前不写 HNS 规则，避免下发部分数据。
	if !proxier.isInitialized() {
		klog.V(2).InfoS("Not syncing hns until Services and Endpoints have been received from master")
		return
	}

	ipFamilyStr := string(proxier.ipFamily)

	// 记录本轮同步耗时。
	start := time.Now()
	defer func() {
		metrics.SyncProxyRulesLatency.WithLabelValues(ipFamilyStr).Observe(metrics.SinceInSeconds(start))
		klog.V(4).InfoS("Syncing proxy rules complete", "elapsed", time.Since(start))
	}()

	hnsNetworkName := proxier.network.name
	hns := proxier.hns

	var gatewayHnsendpoint *endpointInfo
	if proxier.forwardHealthCheckVip {
		gatewayHnsendpoint, _ = hns.getEndpointByName(proxier.rootHnsEndpointName)
	}

	prevNetworkID := proxier.network.id
	updatedNetwork, err := hns.getNetworkByName(hnsNetworkName)
	if IsHnsNotRunningError(err) {
		klog.V(1).ErrorS(err, "HNS is not running. Unable to read network. Skipping sync", "hnsNetworkName", hnsNetworkName)
		return
	}

	if updatedNetwork == nil || updatedNetwork.id != prevNetworkID || isNetworkNotFoundError(err) {
		klog.InfoS("The HNS network is not present or has changed since the last sync, please check the CNI deployment", "hnsNetworkName", hnsNetworkName)
		proxier.cleanupAllPolicies()
		if updatedNetwork != nil {
			proxier.network = *updatedNetwork
		}
		return
	}

	// 清空上一轮的已终止端点记录，本轮重新统计。
	clear(proxier.terminatedEndpoints)

	// 调用本函数即代表需要完整同步，即使期间没有变化；
	// 识别无变化的情况是调用方的责任。
	_ = proxier.svcPortMap.Update(proxier.serviceChanges)
	_ = proxier.endpointsMap.Update(proxier.endpointsChanges)

	// 查询 HNS 中现有的端点与负载均衡器，作为对账基准。
	queriedEndpoints, remoteEPsWithDupIP, err := hns.getAllEndpointsByNetwork(hnsNetworkName)
	defer hns.deleteAllRemoteEndpointsWithDupIP(remoteEPsWithDupIP)

	if err != nil {
		klog.ErrorS(err, "Querying HNS for endpoints failed")
		return
	}
	if queriedEndpoints == nil {
		klog.V(4).InfoS("No existing endpoints found in HNS")
		queriedEndpoints = make(map[string]*(endpointInfo))
	}
	queriedLoadBalancers, err := hns.getAllLoadBalancers()
	if queriedLoadBalancers == nil {
		klog.V(4).InfoS("No existing load balancers found in HNS")
		queriedLoadBalancers = make(map[loadBalancerIdentifier]*(loadBalancerInfo))
	}
	if err != nil {
		klog.ErrorS(err, "Querying HNS for load balancers failed")
		return
	}
	if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) {
		if _, ok := queriedEndpoints[proxier.sourceVip]; !ok {
			_, err = newSourceVIP(hns, hnsNetworkName, proxier.sourceVip, proxier.hostMac, proxier.nodeIP.String())
			if err != nil {
				klog.ErrorS(err, "Source Vip endpoint creation failed")
				return
			}
		}
	}

	klog.V(3).InfoS("Syncing Policies", "proxierFamily", ipFamilyStr, "serviceCount", len(proxier.svcPortMap), "endpointCount", len(proxier.endpointsMap), "queriedEndpointsCount", len(queriedEndpoints), "queriedLoadBalancersCount", len(queriedLoadBalancers))

	defer klog.V(3).InfoS("Syncing Policies complete", "proxierFamily", ipFamilyStr)

	// 为每个服务下发对应的 HNS 策略。
	for svcName, svc := range proxier.svcPortMap {
		svcInfo, ok := svc.(*serviceInfo)
		if !ok {
			klog.ErrorS(nil, "Failed to cast serviceInfo", "serviceName", svcName)
			continue
		}

		if svcInfo.policyApplied {
			klog.V(4).InfoS("Policy already applied", "serviceInfo", svcInfo)
			continue
		}

		if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) {
			serviceVipEndpoint := queriedEndpoints[svcInfo.ClusterIP().String()]
			if serviceVipEndpoint == nil {
				klog.V(4).InfoS("No existing remote endpoint", "IP", svcInfo.ClusterIP())
				hnsEndpoint := &endpointInfo{
					ip:              svcInfo.ClusterIP().String(),
					isLocal:         false,
					macAddress:      proxier.hostMac,
					providerAddress: proxier.nodeIP.String(),
				}

				newHnsEndpoint, err := hns.createEndpoint(hnsEndpoint, hnsNetworkName)
				if err != nil {
					klog.ErrorS(err, "Remote endpoint creation failed for service VIP")
					continue
				}

				newHnsEndpoint.refCount = proxier.endPointsRefCount.getRefCount(newHnsEndpoint.hnsID)
				*newHnsEndpoint.refCount++
				svcInfo.remoteEndpoint = newHnsEndpoint
				updateQueriedEndpoints(newHnsEndpoint, queriedEndpoints)
			}
		}

		var hnsEndpoints []endpointInfo
		var hnsLocalEndpoints []endpointInfo
		klog.V(4).InfoS("Applying Policy", "serviceInfo", svcName)
		// 为服务的每个后端创建对应的远端端点。
		containsPublicIP := false
		containsNodeIP := false
		var allEndpointsTerminating, allEndpointsNonServing bool
		someEndpointsServing := true

		if len(svcInfo.loadBalancerIngressIPs) > 0 {
			// 仅 LoadBalancer 类型服务需要检查端点终止状态，避免无谓开销。
			allEndpointsTerminating = proxier.isAllEndpointsTerminating(svcName, svcInfo.localTrafficDSR)
			allEndpointsNonServing = proxier.isAllEndpointsNonServing(svcName, svcInfo.localTrafficDSR)
			someEndpointsServing = !allEndpointsNonServing
			klog.V(4).InfoS("Terminating status checked for all endpoints", "svcClusterIP", svcInfo.ClusterIP(), "allEndpointsTerminating", allEndpointsTerminating, "allEndpointsNonServing", allEndpointsNonServing, "localTrafficDSR", svcInfo.localTrafficDSR)
		} else {
			klog.V(4).InfoS("Skipped terminating status check for all endpoints", "svcClusterIP", svcInfo.ClusterIP(), "ingressLBCount", len(svcInfo.loadBalancerIngressIPs))
		}

		for _, epInfo := range proxier.endpointsMap[svcName] {
			ep, ok := epInfo.(*endpointInfo)
			if !ok {
				klog.ErrorS(nil, "Failed to cast endpointInfo", "serviceName", svcName)
				continue
			}

			if svcInfo.internalTrafficLocal && svcInfo.localTrafficDSR && !ep.IsLocal() {
				// 内外流量策略均为 Local 时无需使用或创建远端端点。
				klog.V(3).InfoS("Skipping the endpoint. Both internalTraffic and external traffic policies are local", "EpIP", ep.ip, " EpPort", ep.port)
				continue
			}

			if someEndpointsServing {

				if !allEndpointsTerminating && !ep.IsReady() {
					klog.V(3).InfoS("Skipping the endpoint for LB creation. Endpoint is either not ready or all not all endpoints are terminating", "EpIP", ep.ip, " EpPort", ep.port, "allEndpointsTerminating", allEndpointsTerminating, "IsEpReady", ep.IsReady())
					continue
				}
				if !ep.IsServing() {
					klog.V(3).InfoS("Skipping the endpoint for LB creation. Endpoint is not serving", "EpIP", ep.ip, " EpPort", ep.port, "IsEpServing", ep.IsServing())
					continue
				}

			}

			var newHnsEndpoint *endpointInfo
			hnsNetworkName := proxier.network.name
			var err error

			// TargetPort 以名称指定时为 0，真实端口需从端点获取。
			// hnslib.AddLoadBalancer() 不支持不同端口的端点，因此只采用第一个端点的端口。
			// TODO(feiskyer): 待 hnslib.AddLoadBalancer() 支持后再放开不同端点端口。
			if svcInfo.targetPort == 0 || svcInfo.targetPort != int(ep.port) {
				// TargetPort 未指定或与端点端口不同时，以第一个端点的端口为准。
				klog.V(3).InfoS("Update targetPort", "oldTargetPort", svcInfo.targetPort, "newTargetPort", ep.port)
				svcInfo.targetPort = int(ep.port)
			}
			// Windows Server 2019 的缺陷可能创建同 IP 的两个端点，因此先按端点 ID 查找。
			// TODO: 去掉按 ID 查找，仅按 IP 查询，以减少需要维护的键。
			if len(ep.hnsID) > 0 {
				newHnsEndpoint = queriedEndpoints[ep.hnsID]
			}

			if newHnsEndpoint == nil {
				// 再按 IP 查找：本地端点可能已由其他组件创建，或代理重启前已创建远端端点。
				newHnsEndpoint = queriedEndpoints[ep.IP()]
			}

			if newHnsEndpoint == nil {
				if ep.IsLocal() {
					klog.ErrorS(err, "Local endpoint not found: on network", "ip", ep.IP(), "hnsNetworkName", hnsNetworkName)
					continue
				}

				if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) {
					// 重新查询网络，以获取新下发的远程子网策略。
					klog.InfoS("Updating network to check for new remote subnet policies", "networkName", proxier.network.name)
					networkName := proxier.network.name
					updatedNetwork, err := hns.getNetworkByName(networkName)
					if err != nil {
						klog.ErrorS(err, "Unable to find HNS Network specified, please check network name and CNI deployment", "hnsNetworkName", hnsNetworkName)
						proxier.cleanupAllPolicies()
						return
					}
					proxier.network = *updatedNetwork
					providerAddress := proxier.network.findRemoteSubnetProviderAddress(ep.IP())
					if len(providerAddress) == 0 {
						klog.InfoS("Could not find provider address, assuming it is a public IP", "IP", ep.IP())
						providerAddress = proxier.nodeIP.String()
					}

					hnsEndpoint := &endpointInfo{
						ip:              ep.ip,
						isLocal:         false,
						macAddress:      conjureMac("02-11", netutils.ParseIPSloppy(ep.ip)),
						providerAddress: providerAddress,
					}

					newHnsEndpoint, err = hns.createEndpoint(hnsEndpoint, hnsNetworkName)
					if err != nil {
						klog.ErrorS(err, "Remote endpoint creation failed", "endpointInfo", hnsEndpoint)
						continue
					}
					updateQueriedEndpoints(newHnsEndpoint, queriedEndpoints)
				} else {

					hnsEndpoint := &endpointInfo{
						ip:         ep.ip,
						isLocal:    false,
						macAddress: ep.macAddress,
					}

					newHnsEndpoint, err = hns.createEndpoint(hnsEndpoint, hnsNetworkName)
					if err != nil {
						klog.ErrorS(err, "Remote endpoint creation failed")
						continue
					}
					updateQueriedEndpoints(newHnsEndpoint, queriedEndpoints)
				}
			}
			// Overlay 网络的 SourceVIP 有两种选择：kube-proxy 配置的 SourceVIP，或当前节点 IP。
			// L2Bridge 网络则始终使用当前节点 IP 作为 SourceVIP。
			// Overlay 下的选择依据后端端点：集群外 IP 或远端节点 IP 使用节点 IP；
			// 其余情况（本地/远端 Pod、当前节点 IP）使用配置的 SourceVIP。
			if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) && !ep.IsLocal() {
				providerAddress := proxier.network.findRemoteSubnetProviderAddress(ep.IP())

				isNodeIP := (ep.IP() == providerAddress)
				isPublicIP := (len(providerAddress) == 0)
				klog.InfoS("Endpoint on overlay network", "ip", ep.IP(), "hnsNetworkName", hnsNetworkName, "isNodeIP", isNodeIP, "isPublicIP", isPublicIP)

				containsNodeIP = containsNodeIP || isNodeIP
				containsPublicIP = containsPublicIP || isPublicIP
			}

			// 记录 HNS 端点 ID 供后续引用。
			klog.V(1).InfoS("Hns endpoint resource", "endpointInfo", newHnsEndpoint)

			hnsEndpoints = append(hnsEndpoints, *newHnsEndpoint)
			if newHnsEndpoint.IsLocal() {
				hnsLocalEndpoints = append(hnsLocalEndpoints, *newHnsEndpoint)
			} else {
				// 只有远端端点需要共享引用计数，本地端点由其他组件管理。
				ep.refCount = proxier.endPointsRefCount.getRefCount(newHnsEndpoint.hnsID)
				*ep.refCount++
			}

			ep.hnsID = newHnsEndpoint.hnsID

			klog.V(3).InfoS("Endpoint resource found", "endpointInfo", ep)
		}

		klog.V(3).InfoS("Associated endpoints for service", "endpointInfo", fmt.Sprintf("%v", hnsEndpoints), "serviceName", svcName)

		if len(svcInfo.hnsID) > 0 {
			// 正常流程下不应出现已有策略仍待创建的情况。
			klog.InfoS("Load Balancer already exists.", "hnsID", svcInfo.hnsID)
		}

		// ETP:Cluster 下所有端点都在终止时仍带有 serving 与 terminating 状态；
		// 否则只需关注 ready 和 serving。
		if len(hnsEndpoints) == 0 {
			if svcInfo.winProxyOptimization {
				// 没有可服务端点时删除已有负载均衡器。
				klog.V(3).InfoS("Cleanup existing", "endpointInfo", hnsEndpoints, "serviceName", svcName)
				svcInfo.deleteLoadBalancerPolicy(proxier.mapStaleLoadbalancers, ipFamilyStr)
			}
			klog.ErrorS(nil, "Endpoint information not available for service, not applying any policy", "serviceName", svcName)
			continue
		}

		klog.V(4).InfoS("Trying to apply Policies for service", "serviceInfo", svcInfo)
		var hnsLoadBalancer *loadBalancerInfo
		var sourceVip = proxier.sourceVip
		if containsPublicIP || containsNodeIP {
			sourceVip = proxier.nodeIP.String()
		}

		sessionAffinityClientIP := svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP
		if sessionAffinityClientIP && !proxier.supportedFeatures.SessionAffinity {
			klog.InfoS("Session Affinity is not supported on this version of Windows")
		}

		endpointsAvailableForLB := !allEndpointsTerminating && !allEndpointsNonServing

		// clusterIPEndpoints 是用于创建 ClusterIP 负载均衡器的后端列表。
		clusterIPEndpoints := hnsEndpoints
		if svcInfo.internalTrafficLocal {
			// 内部流量策略为 Local 时，ClusterIP 策略只使用本地端点。
			clusterIPEndpoints = hnsLocalEndpoints
		}

		if proxier.requiresUpdateLoadbalancer(svcInfo.hnsID, len(clusterIPEndpoints)) {
			hnsLoadBalancer, err = hns.updateLoadBalancer(
				svcInfo.hnsID,
				sourceVip,
				svcInfo.ClusterIP().String(),
				clusterIPEndpoints,
				loadBalancerFlags{isDSR: proxier.isDSR, isIPv6: proxier.ipFamily == v1.IPv6Protocol, sessionAffinity: sessionAffinityClientIP},
				Enum(svcInfo.Protocol()),
				uint16(svcInfo.targetPort),
				uint16(svcInfo.Port()),
				queriedLoadBalancers,
			)
			if skipIteration := proxier.handleUpdateLoadbalancerFailure(err, svcInfo.hnsID, svcInfo.ClusterIP().String(), len(clusterIPEndpoints)); skipIteration {
				metrics.WinKernelLBUpdateFailure.WithLabelValues(ipFamilyStr, string(lbTypeClusterIP), string(classifyLBError(err))).Inc()
				continue
			}
		}

		if !proxier.requiresUpdateLoadbalancer(svcInfo.hnsID, len(clusterIPEndpoints)) {
			proxier.deleteExistingLoadBalancer(hns, svcInfo.winProxyOptimization, &svcInfo.hnsID, svcInfo.ClusterIP().String(), Enum(svcInfo.Protocol()), uint16(svcInfo.targetPort), uint16(svcInfo.Port()), clusterIPEndpoints, queriedLoadBalancers, lbTypeClusterIP)
			if len(clusterIPEndpoints) > 0 {

				// 端点全部处于终止状态时无需创建 ClusterIP 负载均衡器。
				hnsLoadBalancer, err := hns.getLoadBalancer(
					clusterIPEndpoints,
					loadBalancerFlags{isDSR: proxier.isDSR, isIPv6: proxier.ipFamily == v1.IPv6Protocol, sessionAffinity: sessionAffinityClientIP},
					sourceVip,
					svcInfo.ClusterIP().String(),
					Enum(svcInfo.Protocol()),
					uint16(svcInfo.targetPort),
					uint16(svcInfo.Port()),
					queriedLoadBalancers,
				)
				if err != nil {
					klog.ErrorS(err, "ClusterIP policy creation failed", "clusterIP", svcInfo.ClusterIP(), "serviceName", svcName, "proxierIPFamily", proxier.ipFamily)
					metrics.WinKernelLBCreateFailure.WithLabelValues(ipFamilyStr, string(lbTypeClusterIP), string(classifyLBError(err))).Inc()
					continue
				}

				svcInfo.hnsID = hnsLoadBalancer.hnsID
				klog.V(3).InfoS("Hns LoadBalancer resource created for cluster ip resources", "clusterIP", svcInfo.ClusterIP(), "hnsID", hnsLoadBalancer.hnsID, "serviceName", svcName, "proxierIPFamily", proxier.ipFamily)

			} else {
				klog.V(3).InfoS("Skipped creating Hns LoadBalancer for cluster ip resources. Reason : all endpoints are terminating", "clusterIP", svcInfo.ClusterIP(), "nodeport", svcInfo.NodePort(), "allEndpointsTerminating", allEndpointsTerminating)
			}
		}

		// 配置了 NodePort 时，用户应能通过 nodeIP:nodePort 访问后端端点。
		if svcInfo.NodePort() > 0 {
			// 存在 preserve-destination 注解时关闭 NodePort 的路由网格，
			// 避免健康检查得到来自其他节点的虚假结果。
			nodePortEndpoints := hnsEndpoints
			if svcInfo.preserveDIP || svcInfo.localTrafficDSR {
				nodePortEndpoints = hnsLocalEndpoints
			}

			if proxier.requiresUpdateLoadbalancer(svcInfo.nodePorthnsID, len(nodePortEndpoints)) && endpointsAvailableForLB {
				hnsLoadBalancer, err = hns.updateLoadBalancer(
					svcInfo.nodePorthnsID,
					sourceVip,
					"",
					nodePortEndpoints,
					loadBalancerFlags{isVipExternalIP: true, isDSR: svcInfo.localTrafficDSR, localRoutedVIP: true, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.ipFamily == v1.IPv6Protocol},
					Enum(svcInfo.Protocol()),
					uint16(svcInfo.targetPort),
					uint16(svcInfo.NodePort()),
					queriedLoadBalancers,
				)
				if skipIteration := proxier.handleUpdateLoadbalancerFailure(err, svcInfo.nodePorthnsID, sourceVip, len(nodePortEndpoints)); skipIteration {
					metrics.WinKernelLBUpdateFailure.WithLabelValues(ipFamilyStr, string(lbTypeNodePort), string(classifyLBError(err))).Inc()
					continue
				}
			}

			if !proxier.requiresUpdateLoadbalancer(svcInfo.nodePorthnsID, len(nodePortEndpoints)) {
				proxier.deleteExistingLoadBalancer(hns, svcInfo.winProxyOptimization, &svcInfo.nodePorthnsID, "", Enum(svcInfo.Protocol()), uint16(svcInfo.targetPort), uint16(svcInfo.NodePort()), nodePortEndpoints, queriedLoadBalancers, lbTypeNodePort)

				if len(nodePortEndpoints) > 0 && endpointsAvailableForLB {
					// 端点全部处于终止状态时无需创建 NodePort 负载均衡器。
					hnsLoadBalancer, err := hns.getLoadBalancer(
						nodePortEndpoints,
						loadBalancerFlags{isVipExternalIP: true, isDSR: svcInfo.localTrafficDSR, localRoutedVIP: true, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.ipFamily == v1.IPv6Protocol},
						sourceVip,
						"",
						Enum(svcInfo.Protocol()),
						uint16(svcInfo.targetPort),
						uint16(svcInfo.NodePort()),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "Nodeport policy creation failed", "nodeport", svcInfo.NodePort(), "sourceVip", sourceVip, "proxierIPFamily", proxier.ipFamily, "nodeIP", proxier.nodeIP.String())
						metrics.WinKernelLBCreateFailure.WithLabelValues(ipFamilyStr, string(lbTypeNodePort), string(classifyLBError(err))).Inc()
						continue
					}

					svcInfo.nodePorthnsID = hnsLoadBalancer.hnsID
					klog.V(3).InfoS("Hns LoadBalancer resource created for nodePort resources", "clusterIP", svcInfo.ClusterIP(), "nodeport", svcInfo.NodePort(), "hnsID", hnsLoadBalancer.hnsID, "sourceVip", sourceVip, "proxierIPFamily", proxier.ipFamily, "nodeIP", proxier.nodeIP.String())
				} else {
					klog.V(3).InfoS("Skipped creating Hns LoadBalancer for nodePort resources", "clusterIP", svcInfo.ClusterIP(), "nodeport", svcInfo.NodePort(), "allEndpointsTerminating", allEndpointsTerminating)
				}
			}
		} else {
			klog.V(3).InfoS("Skipped configuring NodePort for service", "serviceName", svcName, "nodePort", svcInfo.NodePort(), "proxierIPFamily", proxier.ipFamily)
		}

		// 为每个 ExternalIP 创建一条负载均衡器策略。
		for _, externalIP := range svcInfo.externalIPs {
			// ExternalTrafficPolicy 为 Local 时关闭路由网格。
			externalIPEndpoints := hnsEndpoints
			if svcInfo.localTrafficDSR {
				externalIPEndpoints = hnsLocalEndpoints
			}

			if proxier.requiresUpdateLoadbalancer(externalIP.hnsID, len(externalIPEndpoints)) && endpointsAvailableForLB {
				hnsLoadBalancer, err = hns.updateLoadBalancer(
					externalIP.hnsID,
					sourceVip,
					externalIP.ip,
					externalIPEndpoints,
					loadBalancerFlags{isVipExternalIP: true, isDSR: svcInfo.localTrafficDSR, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.ipFamily == v1.IPv6Protocol},
					Enum(svcInfo.Protocol()),
					uint16(svcInfo.targetPort),
					uint16(svcInfo.Port()),
					queriedLoadBalancers,
				)
				if skipIteration := proxier.handleUpdateLoadbalancerFailure(err, externalIP.hnsID, externalIP.ip, len(externalIPEndpoints)); skipIteration {
					metrics.WinKernelLBUpdateFailure.WithLabelValues(ipFamilyStr, string(lbTypeExternalIP), string(classifyLBError(err))).Inc()
					continue
				}
			}

			if !proxier.requiresUpdateLoadbalancer(externalIP.hnsID, len(externalIPEndpoints)) {
				proxier.deleteExistingLoadBalancer(hns, svcInfo.winProxyOptimization, &externalIP.hnsID, externalIP.ip, Enum(svcInfo.Protocol()), uint16(svcInfo.targetPort), uint16(svcInfo.Port()), externalIPEndpoints, queriedLoadBalancers, lbTypeExternalIP)

				if len(externalIPEndpoints) > 0 && endpointsAvailableForLB {
					// 端点全部处于终止状态时无需创建 ExternalIP 负载均衡器；
					// 否则先尝试复用已有策略。
					hnsLoadBalancer, err = hns.getLoadBalancer(
						externalIPEndpoints,
						loadBalancerFlags{isVipExternalIP: true, isDSR: svcInfo.localTrafficDSR, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.ipFamily == v1.IPv6Protocol},
						sourceVip,
						externalIP.ip,
						Enum(svcInfo.Protocol()),
						uint16(svcInfo.targetPort),
						uint16(svcInfo.Port()),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "ExternalIP policy creation failed", "externalIP", externalIP.ip)
						metrics.WinKernelLBCreateFailure.WithLabelValues(ipFamilyStr, string(lbTypeExternalIP), string(classifyLBError(err))).Inc()
						continue
					}
					externalIP.hnsID = hnsLoadBalancer.hnsID
					klog.V(3).InfoS("Hns LoadBalancer resource created for externalIP resources", "externalIPInfo", externalIP, "hnsID", hnsLoadBalancer.hnsID)
				} else {
					klog.V(3).InfoS("Skipped creating Hns LoadBalancer for externalIP resources", "externalIPInfo", externalIP, "allEndpointsTerminating", allEndpointsTerminating)
				}
			}
		}

		// 为每个 LoadBalancer Ingress IP 创建一条负载均衡器策略。
		for _, lbIngressIP := range svcInfo.loadBalancerIngressIPs {
			// 先尝试复用已有策略。
			lbIngressEndpoints := hnsEndpoints
			if svcInfo.preserveDIP || svcInfo.localTrafficDSR {
				lbIngressEndpoints = hnsLocalEndpoints
			}

			if proxier.requiresUpdateLoadbalancer(lbIngressIP.hnsID, len(lbIngressEndpoints)) {
				hnsLoadBalancer, err = hns.updateLoadBalancer(
					lbIngressIP.hnsID,
					sourceVip,
					lbIngressIP.ip,
					lbIngressEndpoints,
					loadBalancerFlags{isVipExternalIP: true, isDSR: svcInfo.preserveDIP || svcInfo.localTrafficDSR, useMUX: svcInfo.preserveDIP, preserveDIP: svcInfo.preserveDIP, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.ipFamily == v1.IPv6Protocol},
					Enum(svcInfo.Protocol()),
					uint16(svcInfo.targetPort),
					uint16(svcInfo.Port()),
					queriedLoadBalancers,
				)
				if skipIteration := proxier.handleUpdateLoadbalancerFailure(err, lbIngressIP.hnsID, lbIngressIP.ip, len(lbIngressEndpoints)); skipIteration {
					metrics.WinKernelLBUpdateFailure.WithLabelValues(ipFamilyStr, string(lbTypeIngressIP), string(classifyLBError(err))).Inc()
					continue
				}
			}

			if !proxier.requiresUpdateLoadbalancer(lbIngressIP.hnsID, len(lbIngressEndpoints)) {
				proxier.deleteExistingLoadBalancer(hns, svcInfo.winProxyOptimization, &lbIngressIP.hnsID, lbIngressIP.ip, Enum(svcInfo.Protocol()), uint16(svcInfo.targetPort), uint16(svcInfo.Port()), lbIngressEndpoints, queriedLoadBalancers, lbTypeIngressIP)

				if len(lbIngressEndpoints) > 0 {
					hnsLoadBalancer, err := hns.getLoadBalancer(
						lbIngressEndpoints,
						loadBalancerFlags{isVipExternalIP: true, isDSR: svcInfo.preserveDIP || svcInfo.localTrafficDSR, useMUX: svcInfo.preserveDIP, preserveDIP: svcInfo.preserveDIP, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.ipFamily == v1.IPv6Protocol},
						sourceVip,
						lbIngressIP.ip,
						Enum(svcInfo.Protocol()),
						uint16(svcInfo.targetPort),
						uint16(svcInfo.Port()),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "IngressIP policy creation failed", "lbIngressIP", lbIngressIP.ip, "serviceName", svcName, "proxierIPFamily", proxier.ipFamily)
						metrics.WinKernelLBCreateFailure.WithLabelValues(ipFamilyStr, string(lbTypeIngressIP), string(classifyLBError(err))).Inc()
						continue
					}
					lbIngressIP.hnsID = hnsLoadBalancer.hnsID
					klog.V(3).InfoS("Hns LoadBalancer resource created for loadBalancer Ingress resources", "lbIngressIPInfo", lbIngressIP, "hnsID", hnsLoadBalancer.hnsID, "serviceName", svcName, "proxierIPFamily", proxier.ipFamily)
				} else {
					klog.V(3).InfoS("Skipped creating Hns LoadBalancer for loadBalancer Ingress resources", "lbIngressIPInfo", lbIngressIP)
				}
			}

			if proxier.forwardHealthCheckVip && gatewayHnsendpoint != nil && endpointsAvailableForLB {
				// 端点全部处于终止状态时不创建健康检查负载均衡器。
				nodeport := proxier.healthzPort
				if svcInfo.HealthCheckNodePort() != 0 {
					nodeport = svcInfo.HealthCheckNodePort()
				}

				gwEndpoints := []endpointInfo{*gatewayHnsendpoint}

				if proxier.requiresUpdateLoadbalancer(lbIngressIP.healthCheckHnsID, len(gwEndpoints)) {
					hnsLoadBalancer, err = hns.updateLoadBalancer(
						lbIngressIP.healthCheckHnsID,
						sourceVip,
						lbIngressIP.ip,
						gwEndpoints,
						loadBalancerFlags{isDSR: false, useMUX: svcInfo.preserveDIP, preserveDIP: svcInfo.preserveDIP},
						Enum(svcInfo.Protocol()),
						uint16(nodeport),
						uint16(nodeport),
						queriedLoadBalancers,
					)
					if skipIteration := proxier.handleUpdateLoadbalancerFailure(err, lbIngressIP.healthCheckHnsID, lbIngressIP.ip, 1); skipIteration {
						continue
					}
				}

				if !proxier.requiresUpdateLoadbalancer(lbIngressIP.healthCheckHnsID, len(gwEndpoints)) {
					proxier.deleteExistingLoadBalancer(hns, svcInfo.winProxyOptimization, &lbIngressIP.healthCheckHnsID, lbIngressIP.ip, Enum(svcInfo.Protocol()), uint16(nodeport), uint16(nodeport), gwEndpoints, queriedLoadBalancers, lbTypeHealthCheck)

					hnsHealthCheckLoadBalancer, err := hns.getLoadBalancer(
						gwEndpoints,
						loadBalancerFlags{isDSR: false, useMUX: svcInfo.preserveDIP, preserveDIP: svcInfo.preserveDIP},
						sourceVip,
						lbIngressIP.ip,
						Enum(svcInfo.Protocol()),
						uint16(nodeport),
						uint16(nodeport),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "Healthcheck loadbalancer policy creation failed", "lbIngressIP", lbIngressIP.ip, "serviceName", svcName, "proxierIPFamily", proxier.ipFamily)
						continue
					}
					lbIngressIP.healthCheckHnsID = hnsHealthCheckLoadBalancer.hnsID
					klog.V(3).InfoS("Hns Health Check LoadBalancer resource created for loadBalancer Ingress resources", "ip", lbIngressIP)
				}
			} else {
				klog.V(3).InfoS("Skipped creating Hns Health Check LoadBalancer for loadBalancer Ingress resources", "IngressIPInfo", lbIngressIP, "allEndpointsTerminating", allEndpointsTerminating)
			}
		}
		svcInfo.policyApplied = true
		klog.V(2).InfoS("Policy successfully applied for service", "serviceInfo", svcInfo)
	}

	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated(proxier.ipFamily)
	}
	metrics.SyncProxyRulesLastTimestamp.WithLabelValues(ipFamilyStr).SetToCurrentTime()

	// 更新健康检查：端点列表可能包含非 OnlyLocal 的服务，但服务列表不包含；
	// serviceHealthServer 会自动忽略这些端点。
	if err := proxier.serviceHealthServer.SyncServices(proxier.svcPortMap.HealthCheckNodePorts()); err != nil {
		klog.ErrorS(err, "Error syncing healthcheck services")
	}
	if err := proxier.serviceHealthServer.SyncEndpoints(proxier.endpointsMap.LocalReadyEndpoints()); err != nil {
		klog.ErrorS(err, "Error syncing healthcheck endpoints")
	}

	// 清理已不再使用的端点引用计数条目。
	proxier.deleteTerminatedEndpoints(queriedEndpoints)

	// 清理上一轮删除失败、等待重试的陈旧负载均衡器。
	proxier.cleanupStaleLoadbalancers()
	return
}

// deleteTerminatedEndpoints 删除已终止且不再被任何服务引用的远端端点。
func (proxier *Proxier) deleteTerminatedEndpoints(queriedEndpoints map[string]*(endpointInfo)) {
	for epIP := range proxier.terminatedEndpoints {
		klog.V(5).InfoS("Terminated endpoints ready for deletion", "epIP", epIP)
		if epToDelete := queriedEndpoints[epIP]; epToDelete != nil && epToDelete.hnsID != "" && !epToDelete.IsLocal() {
			refCount := proxier.endPointsRefCount.getRefCount(epToDelete.hnsID)
			if refCount == nil || *refCount == 0 {
				if err := proxier.hns.deleteEndpoint(epToDelete.hnsID); err != nil {
					klog.ErrorS(err, "Deleting unreferenced remote endpoint failed", "hnsID", epToDelete.hnsID)
				} else {
					klog.V(3).InfoS("Deleting unreferenced remote endpoint succeeded", "hnsID", epToDelete.hnsID, "IP", epToDelete.ip)
				}
			} else {
				klog.V(3).InfoS("Not deleting remote endpoint as it is still referenced", "hnsID", epToDelete.hnsID, "IP", epToDelete.ip, "refCount", refCount)
			}
		}
	}
}

// deleteExistingLoadBalancer 判断是否需要删除旧策略：需要时执行删除并返回 true。
func (proxier *Proxier) deleteExistingLoadBalancer(hns HostNetworkService, winProxyOptimization bool, lbHnsID *string, vip string, protocol, intPort, extPort uint16, endpoints []endpointInfo, queriedLoadBalancers map[loadBalancerIdentifier]*loadBalancerInfo, lbType loadBalancerType) bool {

	if !winProxyOptimization || *lbHnsID == "" {
		// 未开启优化或尚无策略 ID，无需删除。
		return false
	}

	lbID, lbIdErr := findLoadBalancerID(
		endpoints,
		vip,
		protocol,
		intPort,
		extPort,
		proxier.ipFamily == v1.IPv6Protocol,
	)

	if lbIdErr != nil {
		return proxier.deleteLoadBalancer(hns, lbHnsID, lbType)
	}

	if _, ok := queriedLoadBalancers[lbID]; ok {
		// 系统中已有与待删重建完全一致的策略，跳过删除以避免震荡。
		return false
	}

	return proxier.deleteLoadBalancer(hns, lbHnsID, lbType)
}

// deleteLoadBalancer 删除指定策略，失败时记入陈旧列表供后续清理。
func (proxier *Proxier) deleteLoadBalancer(hns HostNetworkService, lbHnsID *string, lbType loadBalancerType) bool {
	klog.V(3).InfoS("Hns LoadBalancer delete triggered for loadBalancer resources", "lbHnsID", *lbHnsID, "lbType", lbType, "proxierIPFamily", proxier.ipFamily)
	if err := hns.deleteLoadBalancer(*lbHnsID); err != nil {
		klog.ErrorS(err, "LoadBalancer deletion failed. Adding to stale loadbalancers.", "hnsID", *lbHnsID)
		metrics.WinKernelLBDeleteFailure.WithLabelValues(string(proxier.ipFamily), string(lbType), string(classifyLBError(err))).Inc()
		// 由 cleanupStaleLoadbalancers 在后续同步中重试清理。
		proxier.mapStaleLoadbalancers[*lbHnsID] = lbType
	}
	*lbHnsID = ""
	return true
}

// localHostMacProvider 通过本机网卡信息查询宿主机 MAC 地址。
type localHostMacProvider struct{}

// GetHostMac 返回与节点 IP 绑定的网卡 MAC 地址，找不到时返回空串。
func (r *localHostMacProvider) GetHostMac(nodeIP net.IP) string {
	var hostMac string
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		addresses, _ := inter.Addrs()
		for _, addr := range addresses {
			addrIP, _, _ := netutils.ParseCIDRSloppy(addr.String())
			if addrIP.String() == nodeIP.String() {
				klog.V(2).InfoS("Record Host MAC address", "addr", inter.HardwareAddr)
				hostMac = inter.HardwareAddr.String()
			}
		}
	}
	return hostMac
}
