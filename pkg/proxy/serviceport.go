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

package proxy

import (
	"fmt"
	"net"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	apiservice "k8s.io/kubernetes/pkg/api/v1/service"
	"k8s.io/kubernetes/pkg/features"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	netutils "k8s.io/utils/net"
)

// ServicePort 是抽象服务信息的接口。
type ServicePort interface {
	// String 返回服务的字符串表示，示例格式为：`IP:Port/Protocol`。
	String() string
	// ClusterIP 以 net.IP 格式返回服务的集群 IP。
	ClusterIP() net.IP
	// Port 返回服务端口；返回 0 表示未设置端口。
	Port() int
	// SessionAffinityType 返回服务的会话亲和性类型
	SessionAffinityType() v1.ServiceAffinity
	// StickyMaxAgeSeconds 返回服务的最大连接保持时长（秒）
	StickyMaxAgeSeconds() int
	// ExternalIPs 返回服务的 ExternalIPs
	ExternalIPs() []net.IP
	// LoadBalancerVIPs 返回服务中处于 VIP 模式的 LoadBalancerIP。
	LoadBalancerVIPs() []net.IP
	// Protocol 返回服务协议。
	Protocol() v1.Protocol
	// LoadBalancerSourceRanges 返回服务的 LoadBalancerSourceRanges；未设置时返回空数组。
	LoadBalancerSourceRanges() []*net.IPNet
	// HealthCheckNodePort 返回服务的健康检查节点端口；返回 0 表示未设置。
	HealthCheckNodePort() int
	// NodePort 返回服务的节点端口；返回 0 表示未设置。
	NodePort() int
	// ExternalPolicyLocal 返回服务对外部流量是否仅使用节点本地端点。
	ExternalPolicyLocal() bool
	// InternalPolicyLocal 返回服务对内部流量是否仅使用节点本地端点。
	InternalPolicyLocal() bool
	// ExternallyAccessible 返回该服务端口是否可通过 ClusterIP 之外的方式
	// （NodePort/ExternalIP/LoadBalancer）访问。
	ExternallyAccessible() bool
	// UsesClusterEndpoints 返回该服务端口是否会基于 "Cluster" 流量策略
	// 向端点发送流量。
	UsesClusterEndpoints() bool
	// UsesLocalEndpoints 返回该服务端口是否会基于 "Local" 流量策略
	// 向端点发送流量。
	UsesLocalEndpoints() bool
}

// BaseServicePortInfo 包含定义一个服务所需的基础信息。
// proxier 处理服务时可以直接使用它，也可以在需要时基于它
// 构造各 proxier 自定义的、更具体的 ServiceInfo 结构。
type BaseServicePortInfo struct {
	clusterIP                net.IP
	port                     int
	protocol                 v1.Protocol
	nodePort                 int
	loadBalancerVIPs         []net.IP
	sessionAffinityType      v1.ServiceAffinity
	stickyMaxAgeSeconds      int
	externalIPs              []net.IP
	loadBalancerSourceRanges []*net.IPNet
	healthCheckNodePort      int
	externalPolicyLocal      bool
	internalPolicyLocal      bool
}

// 编译期断言：确保 BaseServicePortInfo 实现了 ServicePort 接口。
var _ ServicePort = &BaseServicePortInfo{}

// String 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) String() string {
	return fmt.Sprintf("%s:%d/%s", bsvcPortInfo.clusterIP, bsvcPortInfo.port, bsvcPortInfo.protocol)
}

// ClusterIP 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) ClusterIP() net.IP {
	return bsvcPortInfo.clusterIP
}

// Port 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) Port() int {
	return bsvcPortInfo.port
}

// SessionAffinityType 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) SessionAffinityType() v1.ServiceAffinity {
	return bsvcPortInfo.sessionAffinityType
}

// StickyMaxAgeSeconds 是 ServicePort 接口的一部分
func (bsvcPortInfo *BaseServicePortInfo) StickyMaxAgeSeconds() int {
	return bsvcPortInfo.stickyMaxAgeSeconds
}

// Protocol 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) Protocol() v1.Protocol {
	return bsvcPortInfo.protocol
}

// LoadBalancerSourceRanges 是 ServicePort 接口的一部分
func (bsvcPortInfo *BaseServicePortInfo) LoadBalancerSourceRanges() []*net.IPNet {
	return bsvcPortInfo.loadBalancerSourceRanges
}

// HealthCheckNodePort 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) HealthCheckNodePort() int {
	return bsvcPortInfo.healthCheckNodePort
}

// NodePort 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) NodePort() int {
	return bsvcPortInfo.nodePort
}

// ExternalIPs 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) ExternalIPs() []net.IP {
	// 只有特性开关启用时才为 externalIPs 下发规则；开关关闭时这些 IP 不应生效。
	if !utilfeature.DefaultFeatureGate.Enabled(features.AllowServiceExternalIPs) {
		return []net.IP{}
	}
	return bsvcPortInfo.externalIPs
}

// LoadBalancerVIPs 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) LoadBalancerVIPs() []net.IP {
	return bsvcPortInfo.loadBalancerVIPs
}

// ExternalPolicyLocal 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) ExternalPolicyLocal() bool {
	return bsvcPortInfo.externalPolicyLocal
}

// InternalPolicyLocal 是 ServicePort 接口的一部分
func (bsvcPortInfo *BaseServicePortInfo) InternalPolicyLocal() bool {
	return bsvcPortInfo.internalPolicyLocal
}

// ExternallyAccessible 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) ExternallyAccessible() bool {
	return bsvcPortInfo.nodePort != 0 || len(bsvcPortInfo.loadBalancerVIPs) != 0 || len(bsvcPortInfo.externalIPs) != 0
}

// UsesClusterEndpoints 是 ServicePort 接口的一部分。
// UsesClusterEndpoints 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) UsesClusterEndpoints() bool {
	// 内部流量策略为 "Cluster"，或者服务接受外部流量时，都需要 Cluster 端点；
	// 即使外部流量策略为 "Local"，也要靠 Cluster 端点来实现短路（short circuiting），
	// 因此两种情况都算作使用 Cluster 端点。
	return !bsvcPortInfo.internalPolicyLocal || bsvcPortInfo.ExternallyAccessible()
}

// UsesLocalEndpoints 是 ServicePort 接口的一部分。
func (bsvcPortInfo *BaseServicePortInfo) UsesLocalEndpoints() bool {
	return bsvcPortInfo.internalPolicyLocal || (bsvcPortInfo.externalPolicyLocal && bsvcPortInfo.ExternallyAccessible())
}

// newBaseServiceInfo 根据 Service、地址族和 ServicePort 构造 BaseServicePortInfo。
// 各类 IP 都按地址族过滤，因为每个 proxier 只负责单一地址族的规则；
// 健康检查端口只在服务确实需要健康检查时才会读取。
func newBaseServiceInfo(service *v1.Service, ipFamily v1.IPFamily, port *v1.ServicePort) *BaseServicePortInfo {
	externalPolicyLocal := apiservice.ExternalPolicyLocal(service)
	internalPolicyLocal := apiservice.InternalPolicyLocal(service)

	var stickyMaxAgeSeconds int
	if service.Spec.SessionAffinity == v1.ServiceAffinityClientIP {
		// kube-apiserver 保证会话亲和类型为 ClientIP 时 SessionAffinityConfig 不为 nil，
		// 因此这里可以直接解引用。
		stickyMaxAgeSeconds = int(*service.Spec.SessionAffinityConfig.ClientIP.TimeoutSeconds)
	}

	clusterIP := proxyutil.GetClusterIPByFamily(ipFamily, service)
	info := &BaseServicePortInfo{
		clusterIP:           netutils.ParseIPSloppy(clusterIP),
		port:                int(port.Port),
		protocol:            port.Protocol,
		nodePort:            int(port.NodePort),
		sessionAffinityType: service.Spec.SessionAffinity,
		stickyMaxAgeSeconds: stickyMaxAgeSeconds,
		externalPolicyLocal: externalPolicyLocal,
		internalPolicyLocal: internalPolicyLocal,
	}

	// 按地址族过滤 ExternalIPs：本 proxier 只处理该地址族的规则。
	ipFamilyMap := proxyutil.MapIPsByIPFamily(service.Spec.ExternalIPs)
	info.externalIPs = ipFamilyMap[ipFamily]

	// 按地址族过滤源地址段；同时处理 LoadBalancerSourceRanges 校验时
	// 错误地允许首尾空白的问题。
	loadBalancerSourceRanges := make([]string, len(service.Spec.LoadBalancerSourceRanges))
	for i, sourceRange := range service.Spec.LoadBalancerSourceRanges {
		loadBalancerSourceRanges[i] = strings.TrimSpace(sourceRange)
	}

	cidrFamilyMap := proxyutil.MapCIDRsByIPFamily(loadBalancerSourceRanges)
	cidrs := cidrFamilyMap[ipFamily]
	// 全零掩码的 CIDR 表示"允许任意来源"，这与未设置 LoadBalancerSourceRanges 等价。
	if slices.ContainsFunc(cidrs, proxyutil.IsZeroCIDR) {
		cidrs = []*net.IPNet{}
	}
	info.loadBalancerSourceRanges = cidrs

	// 按地址族过滤 LoadBalancer Ingress IP。虽然负载均衡器可能把某一地址族的
	// LB IP 上的连接代理到另一地址族的服务 IP，但这与 kube-proxy 无关：
	// 它只为 VIP 模式的负载均衡器创建规则。
	for _, ing := range service.Status.LoadBalancer.Ingress {
		if ing.IP == "" || !proxyutil.IsVIPMode(ing) {
			continue
		}

		ip := netutils.ParseIPSloppy(ing.IP) // 此前已校验该字符串是 IP 地址
		if ingFamily := proxyutil.GetIPFamilyFromIP(ip); ingFamily == ipFamily {
			info.loadBalancerVIPs = append(info.loadBalancerVIPs, ip)
		}
	}

	if apiservice.NeedsHealthCheck(service) {
		p := service.Spec.HealthCheckNodePort
		if p == 0 {
			klog.ErrorS(nil, "Service has no healthcheck nodeport", "service", klog.KObj(service))
		} else {
			info.healthCheckNodePort = int(p)
		}
	}

	return info
}
