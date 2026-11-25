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
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lithammer/dedent"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/conntrack"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	"k8s.io/kubernetes/pkg/proxy/localnodeportproxy"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/kubernetes/pkg/proxy/runner"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	proxyutiltest "k8s.io/kubernetes/pkg/proxy/util/testing"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/knftables"
)

// 使用 NewFakeProxier 的测试约定（各类 IP 的取值范围）：
//
// Pod IP：            10.0.0.0/8
// Service ClusterIP： 172.30.0.0/16
// 节点 IP：           192.168.0.0/24
// 本节点 IP：         192.168.0.2
// Service ExternalIP：192.168.99.0/24
// LoadBalancer IP：   1.2.3.4、5.6.7.8、9.10.11.12
// 集群外 IP：         203.0.113.0/25 之外的 203.0.113.0/24
// LB 源地址范围：     203.0.113.0/25

const testNodeName = "test-node"
const testNodeIP = "192.168.0.2"
const testNodeIPAlt = "192.168.1.2"
const testExternalIP = "192.168.99.11"
const testNodeIPv6 = "2001:db8::1"
const testNodeIPv6Alt = "2001:db8:1::2"
const testExternalClient = "203.0.113.2"
const testExternalClientBlocked = "203.0.113.130"

var testNodeIPs = []string{testNodeIP, testNodeIPAlt, testExternalIP, testNodeIPv6, testNodeIPv6Alt}

// NewFakeProxier 根据 IP 族创建一个带假 nftables 客户端和假网络的 Proxier，
// 供规则生成与报文流测试共用。
func NewFakeProxier(ipFamily v1.IPFamily) (*knftables.Fake, *Proxier) {
	// TODO: 待把 goroutine 的启动重构到 Run() 方法中后，改为调用 NewProxier。
	nftablesFamily := knftables.IPv4Family
	podCIDR := "10.0.0.0/8"
	serviceCIDRs := "172.30.0.0/16"
	if ipFamily == v1.IPv6Protocol {
		nftablesFamily = knftables.IPv6Family
		podCIDR = "fd00:10::/64"
		serviceCIDRs = "fd00:10:96::/112"
	}
	detectLocal := proxyutil.NewDetectLocalByCIDR(podCIDR)
	nodePortAddresses := []string{fmt.Sprintf("%s/32", testNodeIP), fmt.Sprintf("%s/128", testNodeIPv6)}

	networkInterfacer := proxyutiltest.NewFakeNetwork()
	itf := net.Interface{Index: 0, MTU: 0, Name: "lo", HardwareAddr: nil, Flags: 0}
	addrs := []net.Addr{
		&net.IPNet{IP: netutils.ParseIPSloppy("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy("::1/128"), Mask: net.CIDRMask(128, 128)},
	}
	networkInterfacer.AddInterfaceAddr(&itf, addrs)
	itf1 := net.Interface{Index: 1, MTU: 0, Name: "eth0", HardwareAddr: nil, Flags: 0}
	addrs1 := []net.Addr{
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIP), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIPAlt), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testExternalIP), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIPv6), Mask: net.CIDRMask(64, 128)},
		&net.IPNet{IP: netutils.ParseIPSloppy(testNodeIPv6Alt), Mask: net.CIDRMask(64, 128)},
	}
	networkInterfacer.AddInterfaceAddr(&itf1, addrs1)

	nft := knftables.NewFake(nftablesFamily, kubeProxyTable)

	var nodeIP net.IP
	if ipFamily == v1.IPv4Protocol {
		nodeIP = netutils.ParseIPSloppy(testNodeIP)
	} else {
		nodeIP = netutils.ParseIPSloppy(testNodeIPv6)
	}
	p := &Proxier{
		ipFamily:                 ipFamily,
		svcPortMap:               make(proxy.ServicePortMap),
		serviceChanges:           proxy.NewServiceChangeTracker(ipFamily, newServiceInfo, nil),
		endpointsMap:             make(proxy.EndpointsMap),
		endpointsChanges:         proxy.NewEndpointsChangeTracker(ipFamily, testNodeName, newEndpointInfo, nil),
		needFullSync:             true,
		nftables:                 nft,
		masqueradeMark:           "0x4000",
		masqueradeRule:           "mark set mark or 0x4000",
		conntrack:                conntrack.NewFake(),
		localDetector:            detectLocal,
		nodeName:                 testNodeName,
		serviceHealthServer:      healthcheck.NewFakeServiceHealthServer(),
		nodeIP:                   nodeIP,
		nodePortAddresses:        proxyutil.NewNodePortAddresses(ipFamily, nodePortAddresses),
		networkInterfacer:        networkInterfacer,
		staleChains:              make(map[string]time.Time),
		serviceCIDRs:             serviceCIDRs,
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
	p.setInitialized(true)
	p.syncRunner = runner.NewBoundedFrequencyRunner("test-sync-runner", p.syncProxyRules, 0, 30*time.Second, time.Minute)

	return nft, p
}

var baseRules = dedent.Dedent(`
	add table ip kube-proxy { comment "rules for kube-proxy" ; }

	add set ip kube-proxy cluster-ips { type ipv4_addr ; comment "Active ClusterIPs" ; }
	add set ip kube-proxy nodeport-ips { type ipv4_addr ; comment "IPs that accept NodePort traffic" ; }
	add set ip kube-proxy hairpin-connections { type ipv4_addr . ipv4_addr . inet_proto . inet_service ; comment "service hairpin connections" ; }

	add map ip kube-proxy firewall-ips { type ipv4_addr . inet_proto . inet_service : verdict ; comment "destinations that are subject to LoadBalancerSourceRanges" ; }
	add map ip kube-proxy no-endpoint-nodeports { type inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to service nodeports with no endpoints" ; }
	add map ip kube-proxy no-endpoint-services { type ipv4_addr . inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to services with no endpoints" ; }
	add map ip kube-proxy service-ips { type ipv4_addr . inet_proto . inet_service : verdict ; comment "ClusterIP, ExternalIP and LoadBalancer IP traffic" ; }
	add map ip kube-proxy service-nodeports { type inet_proto . inet_service : verdict ; comment "NodePort traffic" ; }

	add chain ip kube-proxy cluster-ips-check
	add chain ip kube-proxy filter-prerouting-pre-dnat { type filter hook prerouting priority -110 ; }
	add chain ip kube-proxy filter-output-pre-dnat { type filter hook output priority -110 ; }
	add chain ip kube-proxy filter-forward { type filter hook forward priority 0 ; }
	add chain ip kube-proxy filter-input { type filter hook input priority 0 ; }
	add chain ip kube-proxy filter-output { type filter hook output priority 0 ; }
	add chain ip kube-proxy firewall-check
	add chain ip kube-proxy masquerading
	add chain ip kube-proxy nat-output { type nat hook output priority -100 ; }
	add chain ip kube-proxy nat-postrouting { type nat hook postrouting priority 100 ; }
	add chain ip kube-proxy nat-prerouting { type nat hook prerouting priority -100 ; }
	add chain ip kube-proxy nodeport-endpoints-check
	add chain ip kube-proxy reject-chain { comment "helper for @no-endpoint-services / @no-endpoint-nodeports" ; }
	add chain ip kube-proxy services
	add chain ip kube-proxy service-endpoints-check

	add rule ip kube-proxy cluster-ips-check ip daddr @cluster-ips reject comment "Reject traffic to invalid ports of ClusterIPs"
	add rule ip kube-proxy cluster-ips-check ip daddr { 172.30.0.0/16 } drop comment "Drop traffic to unallocated ClusterIPs"
	add rule ip kube-proxy filter-prerouting-pre-dnat ct state new jump firewall-check
	add rule ip kube-proxy filter-forward ct state new jump service-endpoints-check
	add rule ip kube-proxy filter-forward ct state new jump cluster-ips-check
	add rule ip kube-proxy filter-input ct state new jump nodeport-endpoints-check
	add rule ip kube-proxy filter-input ct state new jump service-endpoints-check
	add rule ip kube-proxy filter-output ct state new jump service-endpoints-check
	add rule ip kube-proxy filter-output-pre-dnat ct state new jump firewall-check
	add rule ip kube-proxy filter-output ct state new jump cluster-ips-check
	add rule ip kube-proxy firewall-check ip daddr . meta l4proto . th dport vmap @firewall-ips
	add rule ip kube-proxy masquerading mark and 0x4000 != 0 mark set mark xor 0x4000 masquerade fully-random
	add rule ip kube-proxy masquerading ct status dnat ip saddr . ip daddr . meta l4proto . th dport @hairpin-connections masquerade fully-random
	add rule ip kube-proxy nat-output jump services
	add rule ip kube-proxy nat-postrouting jump masquerading
	add rule ip kube-proxy nat-prerouting jump services
	add rule ip kube-proxy nodeport-endpoints-check ip daddr @nodeport-ips meta l4proto . th dport vmap @no-endpoint-nodeports
	add rule ip kube-proxy reject-chain reject
	add rule ip kube-proxy services ip daddr @cluster-ips ip saddr != 10.0.0.0/8 mark set mark or 0x4000 comment "masquerade clusterIP traffic from outside cluster"
	add rule ip kube-proxy services ip daddr . meta l4proto . th dport vmap @service-ips
	add rule ip kube-proxy services ip daddr @nodeport-ips meta l4proto . th dport vmap @service-nodeports
	
	add element ip kube-proxy nodeport-ips { 192.168.0.2 }
	add rule ip kube-proxy service-endpoints-check ip daddr . meta l4proto . th dport vmap @no-endpoint-services
	`)

var baseRulesV6 = dedent.Dedent(`
	add table ip6 kube-proxy { comment "rules for kube-proxy" ; }

	add set ip6 kube-proxy cluster-ips { type ipv6_addr ; comment "Active ClusterIPs" ; }
	add set ip6 kube-proxy nodeport-ips { type ipv6_addr ; comment "IPs that accept NodePort traffic" ; }
	add set ip6 kube-proxy hairpin-connections { type ipv6_addr . ipv6_addr . inet_proto . inet_service ; comment "service hairpin connections" ; }

	add map ip6 kube-proxy firewall-ips { type ipv6_addr . inet_proto . inet_service : verdict ; comment "destinations that are subject to LoadBalancerSourceRanges" ; }
	add map ip6 kube-proxy no-endpoint-nodeports { type inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to service nodeports with no endpoints" ; }
	add map ip6 kube-proxy no-endpoint-services { type ipv6_addr . inet_proto . inet_service : verdict ; comment "vmap to drop or reject packets to services with no endpoints" ; }
	add map ip6 kube-proxy service-ips { type ipv6_addr . inet_proto . inet_service : verdict ; comment "ClusterIP, ExternalIP and LoadBalancer IP traffic" ; }
	add map ip6 kube-proxy service-nodeports { type inet_proto . inet_service : verdict ; comment "NodePort traffic" ; }

	add chain ip6 kube-proxy cluster-ips-check
	add chain ip6 kube-proxy filter-prerouting-pre-dnat { type filter hook prerouting priority -110 ; }
	add chain ip6 kube-proxy filter-output-pre-dnat { type filter hook output priority -110 ; }
	add chain ip6 kube-proxy filter-forward { type filter hook forward priority 0 ; }
	add chain ip6 kube-proxy filter-input { type filter hook input priority 0 ; }
	add chain ip6 kube-proxy filter-output { type filter hook output priority 0 ; }
	add chain ip6 kube-proxy firewall-check
	add chain ip6 kube-proxy masquerading
	add chain ip6 kube-proxy nat-output { type nat hook output priority -100 ; }
	add chain ip6 kube-proxy nat-postrouting { type nat hook postrouting priority 100 ; }
	add chain ip6 kube-proxy nat-prerouting { type nat hook prerouting priority -100 ; }
	add chain ip6 kube-proxy nodeport-endpoints-check
	add chain ip6 kube-proxy reject-chain { comment "helper for @no-endpoint-services / @no-endpoint-nodeports" ; }
	add chain ip6 kube-proxy services
	add chain ip6 kube-proxy service-endpoints-check

	add rule ip6 kube-proxy cluster-ips-check ip6 daddr @cluster-ips reject comment "Reject traffic to invalid ports of ClusterIPs"
	add rule ip6 kube-proxy cluster-ips-check ip6 daddr { fd00:10:96::/112 } drop comment "Drop traffic to unallocated ClusterIPs"
	add rule ip6 kube-proxy filter-prerouting-pre-dnat ct state new jump firewall-check
	add rule ip6 kube-proxy filter-forward ct state new jump service-endpoints-check
	add rule ip6 kube-proxy filter-forward ct state new jump cluster-ips-check
	add rule ip6 kube-proxy filter-input ct state new jump nodeport-endpoints-check
	add rule ip6 kube-proxy filter-input ct state new jump service-endpoints-check
	add rule ip6 kube-proxy filter-output ct state new jump service-endpoints-check
	add rule ip6 kube-proxy filter-output-pre-dnat ct state new jump firewall-check
	add rule ip6 kube-proxy filter-output ct state new jump cluster-ips-check
	add rule ip6 kube-proxy firewall-check ip6 daddr . meta l4proto . th dport vmap @firewall-ips
	add rule ip6 kube-proxy masquerading mark and 0x4000 != 0 mark set mark xor 0x4000 masquerade fully-random
	add rule ip6 kube-proxy masquerading ct status dnat ip6 saddr . ip6 daddr . meta l4proto . th dport @hairpin-connections masquerade fully-random
	add rule ip6 kube-proxy nat-output jump services
	add rule ip6 kube-proxy nat-postrouting jump masquerading
	add rule ip6 kube-proxy nat-prerouting jump services
	add rule ip6 kube-proxy nodeport-endpoints-check ip6 daddr @nodeport-ips meta l4proto . th dport vmap @no-endpoint-nodeports
	add rule ip6 kube-proxy reject-chain reject
	add rule ip6 kube-proxy services ip6 daddr @cluster-ips ip6 saddr != fd00:10::/64 mark set mark or 0x4000 comment "masquerade clusterIP traffic from outside cluster"
	add rule ip6 kube-proxy services ip6 daddr . meta l4proto . th dport vmap @service-ips
	add rule ip6 kube-proxy services ip6 daddr @nodeport-ips meta l4proto . th dport vmap @service-nodeports

	add element ip6 kube-proxy nodeport-ips { 2001:db8::1 }
	add rule ip6 kube-proxy service-endpoints-check ip6 daddr . meta l4proto . th dport vmap @no-endpoint-services
	`)

// TestOverallNFTablesRules 创建多种类型的 Service，并校验生成的 nftables 规则与预期完全一致。
func TestOverallNFTablesRules(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)

	// 启用了 localhost NodePort 特性，但 nodePortAddresses 中不含回环地址，
	// 因此 kube-proxy 会拒绝所有协议的 localhost NodePort 流量，并分别计数。
	fp.localhostNodePortsEnabled = true

	makeServiceMap(fp,
		// 创建 ClusterIP 类型的 Service
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		// 创建使用 Local 流量策略的 LoadBalancer Service
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3001,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "1.2.3.4",
			}}
			svc.Spec.ExternalIPs = []string{"192.168.99.22"}
			svc.Spec.HealthCheckNodePort = 30000
		}),
		// 创建 NodePort 类型的 Service
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3003,
			}}
		}),
		// 创建带 ExternalIP 的 Service
		makeTestService("ns4", "svc4", func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = "172.30.0.44"
			svc.Spec.ExternalIPs = []string{"192.168.99.33"}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       "p80",
				Port:       80,
				Protocol:   v1.ProtocolTCP,
				TargetPort: intstr.FromInt32(80),
			}}
		}),
		// 创建使用 Cluster 流量策略、源地址范围及会话保持的 LoadBalancer Service
		makeTestService("ns5", "svc5", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
			svc.Spec.ClusterIP = "172.30.0.45"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3002,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "5.6.7.8",
			}}
			svc.Spec.HealthCheckNodePort = 30000
			// 刻意保留前导空格：验证非法取值不会导致崩溃，以兼容历史行为。
			svc.Spec.LoadBalancerSourceRanges = []string{" 203.0.113.0/25"}

			svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
			svc.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
				ClientIP: &v1.ClientIPConfig{
					TimeoutSeconds: ptr.To[int32](10800),
				},
			}
		}),
		// 创建没有端点的 ClusterIP Service
		makeTestService("ns6", "svc6", func(svc *v1.Service) {
			svc.Spec.Type = "ClusterIP"
			svc.Spec.ClusterIP = "172.30.0.46"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       "p80",
				Port:       80,
				Protocol:   v1.ProtocolTCP,
				TargetPort: intstr.FromInt32(80),
			}}
		}),
	)
	populateEndpointSlices(fp,
		// 创建 ClusterIP Service 的端点
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 Local 策略 LoadBalancer 的端点。这里故意把 NodeName 设成“错误”值，
		// 使该端点被判定为非本地而忽略，从而覆盖 Local 策略下的过滤逻辑。
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.2"},
				NodeName:  ptr.To("other-node"),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 NodePort Service 的端点
		makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.3"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 ExternalIP Service 的端点
		makeTestEndpointSlice("ns4", "svc4", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.4"},
				NodeName:  ptr.To("other-node"),
			}, {
				Addresses: []string{"10.180.0.5"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// 创建 Cluster 策略 LoadBalancer 的端点
		makeTestEndpointSlice("ns5", "svc5", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.3"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := baseRules + dedent.Dedent(`
		# svc1
		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.180.0.1:80

		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.180.0.1 . 10.180.0.1 . tcp . 80 }

		# svc2
		add chain ip kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80
		add rule ip kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80 meta l4proto tcp dnat to 10.180.0.2:80
		add chain ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80
		add rule ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 ip saddr 10.0.0.0/8 goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit pod traffic"
		add rule ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local mark set mark or 0x4000 comment "masquerade local traffic"
		add rule ip kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit local traffic"

		add element ip kube-proxy cluster-ips { 172.30.0.42 }
		add element ip kube-proxy service-ips { 172.30.0.42 . tcp . 80 : goto service-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip kube-proxy service-ips { 192.168.99.22 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip kube-proxy service-ips { 1.2.3.4 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3001 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }

		add element ip kube-proxy no-endpoint-nodeports { tcp . 3001 comment "ns2/svc2:p80" : drop }
		add element ip kube-proxy no-endpoint-services { 1.2.3.4 . tcp . 80 comment "ns2/svc2:p80" : drop }
		add element ip kube-proxy no-endpoint-services { 192.168.99.22 . tcp . 80 comment "ns2/svc2:p80" : drop }

		# svc3
		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.180.0.3:80
		add chain ip kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80 goto service-4AT6LBPK-ns3/svc3/tcp/p80

		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3003 : goto external-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.180.0.3 . 10.180.0.3 . tcp . 80 }

		# svc4
		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat ip addr . port to numgen random mod 2 map { 0 : 10.180.0.4 . 80 , 1 : 10.180.0.5 . 80 }
		add chain ip kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80 goto service-LAUZTJTB-ns4/svc4/tcp/p80

		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy service-ips { 192.168.99.33 . tcp . 80 : goto external-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.180.0.5 . 10.180.0.5 . tcp . 80 }

		# svc5
		add set ip kube-proxy affinity-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 { type ipv4_addr ; flags dynamic,timeout ; timeout 10800s ; }
		add chain ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80
		add rule ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 ip saddr @affinity-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 goto endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80
		add rule ip kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 }
		add chain ip kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80
		add rule ip kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80 goto service-HVFWP5L3-ns5/svc5/tcp/p80

		add chain ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80
		add rule ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 update @affinity-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 { ip saddr }
		add rule ip kube-proxy endpoint-GTK6MW7G-ns5/svc5/tcp/p80__10.180.0.3/80 meta l4proto tcp dnat to 10.180.0.3:80

		add chain ip kube-proxy firewall-HVFWP5L3-ns5/svc5/tcp/p80
		add rule ip kube-proxy firewall-HVFWP5L3-ns5/svc5/tcp/p80 ip saddr != { 203.0.113.0/25 } drop

		add element ip kube-proxy cluster-ips { 172.30.0.45 }
		add element ip kube-proxy service-ips { 172.30.0.45 . tcp . 80 : goto service-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip kube-proxy service-ips { 5.6.7.8 . tcp . 80 : goto external-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3002 : goto external-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip kube-proxy firewall-ips { 5.6.7.8 . tcp . 80 : goto firewall-HVFWP5L3-ns5/svc5/tcp/p80 }
		# 10.180.0.3:80 is already in hairpin-connections, so we don't want another "add element" line...

		# svc6
		add element ip kube-proxy cluster-ips { 172.30.0.46 }
		add element ip kube-proxy no-endpoint-services { 172.30.0.46 . tcp . 80 comment "ns6/svc6:p80" : goto reject-chain }

		# localhost NodePort rejects
		add map ip kube-proxy localhost-nodeport-reject-ports { type inet_proto . inet_service : verdict ; comment "localhost NodePorts rejected by kube-proxy" ; }
		add counter ip kube-proxy localhost-nodeport-rejected-tcp
		add counter ip kube-proxy localhost-nodeport-rejected-udp
		add counter ip kube-proxy localhost-nodeport-rejected-sctp
		add chain ip kube-proxy localhost-nodeport-reject-tcp
		add chain ip kube-proxy localhost-nodeport-reject-udp
		add chain ip kube-proxy localhost-nodeport-reject-sctp
		add rule ip kube-proxy localhost-nodeport-reject-tcp counter name "localhost-nodeport-rejected-tcp" reject
		add rule ip kube-proxy localhost-nodeport-reject-udp counter name "localhost-nodeport-rejected-udp" reject
		add rule ip kube-proxy localhost-nodeport-reject-sctp counter name "localhost-nodeport-rejected-sctp" reject
		add rule ip kube-proxy nodeport-endpoints-check ip daddr 127.0.0.0/8 meta l4proto . th dport vmap @localhost-nodeport-reject-ports
		add element ip kube-proxy localhost-nodeport-reject-ports { tcp . 3001 : goto localhost-nodeport-reject-tcp }
		add element ip kube-proxy localhost-nodeport-reject-ports { tcp . 3002 : goto localhost-nodeport-reject-tcp }
		add element ip kube-proxy localhost-nodeport-reject-ports { tcp . 3003 : goto localhost-nodeport-reject-tcp }
		`)

	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
}

// TestOverallNFTablesRulesIPv6 是 TestOverallNFTablesRules 的 IPv6 对应版本。
func TestOverallNFTablesRulesIPv6(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv6Protocol)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)

	// 启用了 localhost NodePort 特性，但 nodePortAddresses 中不含回环地址，
	// 因此 kube-proxy 会拒绝所有协议的 localhost NodePort 流量，并分别计数。
	fp.localhostNodePortsEnabled = true

	makeServiceMap(fp,
		// 创建 ClusterIP 类型的 Service
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "fd00:172:30::41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		// 创建使用 Local 流量策略的 LoadBalancer Service
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.ClusterIP = "fd00:172:30::42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3001,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "5555::1",
			}}
			svc.Spec.ExternalIPs = []string{"2001:db8:99::22"}
			svc.Spec.HealthCheckNodePort = 30000
		}),
		// 创建 NodePort 类型的 Service
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = "fd00:172:30::43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3003,
			}}
		}),
		// 创建带 ExternalIP 的 Service
		makeTestService("ns4", "svc4", func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = "fd00:172:30::44"
			svc.Spec.ExternalIPs = []string{"2001:db8:99::33"}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       "p80",
				Port:       80,
				Protocol:   v1.ProtocolTCP,
				TargetPort: intstr.FromInt32(80),
			}}
		}),
		// 创建使用 Cluster 流量策略、源地址范围及会话保持的 LoadBalancer Service
		makeTestService("ns5", "svc5", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
			svc.Spec.ClusterIP = "fd00:172:30::45"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3002,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "5555::2",
			}}
			svc.Spec.HealthCheckNodePort = 30000
			// 刻意保留前导空格：验证非法取值不会导致崩溃，以兼容历史行为。
			svc.Spec.LoadBalancerSourceRanges = []string{" 2001:db8:203::/64"}

			svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
			svc.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
				ClientIP: &v1.ClientIPConfig{
					TimeoutSeconds: new(int32(10800)),
				},
			}
		}),
		// 创建没有端点的 ClusterIP Service
		makeTestService("ns6", "svc6", func(svc *v1.Service) {
			svc.Spec.Type = "ClusterIP"
			svc.Spec.ClusterIP = "fd00:172:30::46"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       "p80",
				Port:       80,
				Protocol:   v1.ProtocolTCP,
				TargetPort: intstr.FromInt32(80),
			}}
		}),
	)
	populateEndpointSlices(fp,
		// 创建 ClusterIP Service 的端点
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv6
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"fd00:10:180::1"},
				NodeName:  new(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     new("p80"),
				Port:     new(int32(80)),
				Protocol: new(v1.ProtocolTCP),
			}}
		}),
		// 创建 Local 策略 LoadBalancer 的端点。这里故意把 NodeName 设成“错误”值，
		// 使该端点被判定为非本地而忽略，从而覆盖 Local 策略下的过滤逻辑。
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv6
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"fd00:10:180::2"},
				NodeName:  new("other-node"),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     new("p80"),
				Port:     new(int32(80)),
				Protocol: new(v1.ProtocolTCP),
			}}
		}),
		// 创建 NodePort Service 的端点
		makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv6
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"fd00:10:180::3"},
				NodeName:  new(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     new("p80"),
				Port:     new(int32(80)),
				Protocol: new(v1.ProtocolTCP),
			}}
		}),
		// 创建 ExternalIP Service 的端点
		makeTestEndpointSlice("ns4", "svc4", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv6
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"fd00:10:180::4"},
				NodeName:  new("other-node"),
			}, {
				Addresses: []string{"fd00:10:180::5"},
				NodeName:  new(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     new("p80"),
				Port:     new(int32(80)),
				Protocol: new(v1.ProtocolTCP),
			}}
		}),
		// 创建 Cluster 策略 LoadBalancer 的端点
		makeTestEndpointSlice("ns5", "svc5", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv6
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"fd00:10:180::3"},
				NodeName:  new(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     new("p80"),
				Port:     new(int32(80)),
				Protocol: new(v1.ProtocolTCP),
			}}
		}),
	)

	if err := fp.syncProxyRules(); err != nil {
		t.Fatalf("syncProxyRules failed: %v", err)
	}

	expected := baseRulesV6 + dedent.Dedent(`
		# svc1
		add chain ip6 kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip6 kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to [fd00:10:180::1]:80

		add element ip6 kube-proxy cluster-ips { fd00:172:30::41 }
		add element ip6 kube-proxy service-ips { fd00:172:30::41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip6 kube-proxy hairpin-connections { fd00:10:180::1 . fd00:10:180::1 . tcp . 80 }

		# svc2
		add chain ip6 kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80
		add rule ip6 kube-proxy service-42NFTM6N-ns2/svc2/tcp/p80 meta l4proto tcp dnat to [fd00:10:180::2]:80
		add chain ip6 kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80
		add rule ip6 kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 ip6 saddr fd00:10::/64 goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit pod traffic"
		add rule ip6 kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local mark set mark or 0x4000 comment "masquerade local traffic"
		add rule ip6 kube-proxy external-42NFTM6N-ns2/svc2/tcp/p80 fib saddr type local goto service-42NFTM6N-ns2/svc2/tcp/p80 comment "short-circuit local traffic"

		add element ip6 kube-proxy cluster-ips { fd00:172:30::42 }
		add element ip6 kube-proxy service-ips { fd00:172:30::42 . tcp . 80 : goto service-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip6 kube-proxy service-ips { 2001:db8:99::22 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip6 kube-proxy service-ips { 5555::1 . tcp . 80 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }
		add element ip6 kube-proxy service-nodeports { tcp . 3001 : goto external-42NFTM6N-ns2/svc2/tcp/p80 }

		add element ip6 kube-proxy no-endpoint-nodeports { tcp . 3001 comment "ns2/svc2:p80" : drop }
		add element ip6 kube-proxy no-endpoint-services { 5555::1 . tcp . 80 comment "ns2/svc2:p80" : drop }
		add element ip6 kube-proxy no-endpoint-services { 2001:db8:99::22 . tcp . 80 comment "ns2/svc2:p80" : drop }

		# svc3
		add chain ip6 kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip6 kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to [fd00:10:180::3]:80
		add chain ip6 kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip6 kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip6 kube-proxy external-4AT6LBPK-ns3/svc3/tcp/p80 goto service-4AT6LBPK-ns3/svc3/tcp/p80

		add element ip6 kube-proxy cluster-ips { fd00:172:30::43 }
		add element ip6 kube-proxy service-ips { fd00:172:30::43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip6 kube-proxy service-nodeports { tcp . 3003 : goto external-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip6 kube-proxy hairpin-connections { fd00:10:180::3 . fd00:10:180::3 . tcp . 80 }

		# svc4
		add chain ip6 kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip6 kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat ip6 addr . port to numgen random mod 2 map { 0 : fd00:10:180::4 . 80 , 1 : fd00:10:180::5 . 80 }
		add chain ip6 kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip6 kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip6 kube-proxy external-LAUZTJTB-ns4/svc4/tcp/p80 goto service-LAUZTJTB-ns4/svc4/tcp/p80

		add element ip6 kube-proxy cluster-ips { fd00:172:30::44 }
		add element ip6 kube-proxy service-ips { fd00:172:30::44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip6 kube-proxy service-ips { 2001:db8:99::33 . tcp . 80 : goto external-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip6 kube-proxy hairpin-connections { fd00:10:180::5 . fd00:10:180::5 . tcp . 80 }

		# svc5
		add set ip6 kube-proxy affinity-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80 { type ipv6_addr ; flags dynamic,timeout ; timeout 10800s ; }
		add chain ip6 kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80
		add rule ip6 kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 ip6 saddr @affinity-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80 goto endpoint-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80
		add rule ip6 kube-proxy service-HVFWP5L3-ns5/svc5/tcp/p80 numgen random mod 1 vmap { 0 : goto endpoint-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80 }
		add chain ip6 kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80
		add rule ip6 kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip6 kube-proxy external-HVFWP5L3-ns5/svc5/tcp/p80 goto service-HVFWP5L3-ns5/svc5/tcp/p80

		add chain ip6 kube-proxy endpoint-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80
		add rule ip6 kube-proxy endpoint-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80 update @affinity-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80 { ip6 saddr }
		add rule ip6 kube-proxy endpoint-5HFFSBBM-ns5/svc5/tcp/p80__fd00.10.180..3/80 meta l4proto tcp dnat to [fd00:10:180::3]:80

		add chain ip6 kube-proxy firewall-HVFWP5L3-ns5/svc5/tcp/p80
		add rule ip6 kube-proxy firewall-HVFWP5L3-ns5/svc5/tcp/p80 ip6 saddr != { 2001:db8:203::/64 } drop

		add element ip6 kube-proxy cluster-ips { fd00:172:30::45 }
		add element ip6 kube-proxy service-ips { fd00:172:30::45 . tcp . 80 : goto service-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip6 kube-proxy service-ips { 5555::2 . tcp . 80 : goto external-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip6 kube-proxy service-nodeports { tcp . 3002 : goto external-HVFWP5L3-ns5/svc5/tcp/p80 }
		add element ip6 kube-proxy firewall-ips { 5555::2 . tcp . 80 : goto firewall-HVFWP5L3-ns5/svc5/tcp/p80 }
		# [fd00:10:180::3]:80 is already in hairpin-connections, so we don't want another "add element" line...

		# svc6
		add element ip6 kube-proxy cluster-ips { fd00:172:30::46 }
		add element ip6 kube-proxy no-endpoint-services { fd00:172:30::46 . tcp . 80 comment "ns6/svc6:p80" : goto reject-chain }

		# localhost NodePort rejects
		add map ip6 kube-proxy localhost-nodeport-reject-ports { type inet_proto . inet_service : verdict ; comment "localhost NodePorts rejected by kube-proxy" ; }
		add counter ip6 kube-proxy localhost-nodeport-rejected-tcp
		add counter ip6 kube-proxy localhost-nodeport-rejected-udp
		add counter ip6 kube-proxy localhost-nodeport-rejected-sctp
		add chain ip6 kube-proxy localhost-nodeport-reject-tcp
		add chain ip6 kube-proxy localhost-nodeport-reject-udp
		add chain ip6 kube-proxy localhost-nodeport-reject-sctp
		add rule ip6 kube-proxy localhost-nodeport-reject-tcp counter name "localhost-nodeport-rejected-tcp" reject
		add rule ip6 kube-proxy localhost-nodeport-reject-udp counter name "localhost-nodeport-rejected-udp" reject
		add rule ip6 kube-proxy localhost-nodeport-reject-sctp counter name "localhost-nodeport-rejected-sctp" reject
		add rule ip6 kube-proxy nodeport-endpoints-check ip6 daddr ::1 meta l4proto . th dport vmap @localhost-nodeport-reject-ports
		add element ip6 kube-proxy localhost-nodeport-reject-ports { tcp . 3001 : goto localhost-nodeport-reject-tcp }
		add element ip6 kube-proxy localhost-nodeport-reject-ports { tcp . 3002 : goto localhost-nodeport-reject-tcp }
		add element ip6 kube-proxy localhost-nodeport-reject-ports { tcp . 3003 : goto localhost-nodeport-reject-tcp }
		`)

	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
}

// TestEndpointDNATRules 校验两种 IP 族下写入各 Service 链的 DNAT 规则。
// 特别是单端点规则必须使用 "dnat to addr:port" 形式：
// "dnat ip6 addr . port to addr . port" 形式会导致 nft 崩溃（https://issues.k8s.io/131605）。
func TestEndpointDNATRules(t *testing.T) {
	testCases := []struct {
		name         string
		family       v1.IPFamily
		svcIP        string
		epIPs        []string
		expectedRule string
	}{
		{
			name:         "ipv4 single endpoint",
			family:       v1.IPv4Protocol,
			svcIP:        "172.30.0.41",
			epIPs:        []string{"10.180.0.1"},
			expectedRule: "meta l4proto tcp dnat to 10.180.0.1:80",
		},
		{
			name:         "ipv6 single endpoint",
			family:       v1.IPv6Protocol,
			svcIP:        "fd00:172:30::41",
			epIPs:        []string{"fd00:10:180::1"},
			expectedRule: "meta l4proto tcp dnat to [fd00:10:180::1]:80",
		},
		{
			name:         "ipv4 multiple endpoints",
			family:       v1.IPv4Protocol,
			svcIP:        "172.30.0.41",
			epIPs:        []string{"10.180.0.1", "10.180.0.2"},
			expectedRule: "meta l4proto tcp dnat ip addr . port to numgen random mod 2 map { 0 : 10.180.0.1 . 80 , 1 : 10.180.0.2 . 80 }",
		},
		{
			name:         "ipv6 multiple endpoints",
			family:       v1.IPv6Protocol,
			svcIP:        "fd00:172:30::41",
			epIPs:        []string{"fd00:10:180::1", "fd00:10:180::2"},
			expectedRule: "meta l4proto tcp dnat ip6 addr . port to numgen random mod 2 map { 0 : fd00:10:180::1 . 80 , 1 : fd00:10:180::2 . 80 }",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nft, fp := NewFakeProxier(tc.family)

			makeServiceMap(fp,
				makeTestService("ns1", "svc1", func(svc *v1.Service) {
					svc.Spec.Type = v1.ServiceTypeClusterIP
					svc.Spec.ClusterIP = tc.svcIP
					svc.Spec.Ports = []v1.ServicePort{{
						Name:     "p80",
						Port:     80,
						Protocol: v1.ProtocolTCP,
					}}
				}),
			)

			populateEndpointSlices(fp,
				makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
					if tc.family == v1.IPv4Protocol {
						eps.AddressType = discovery.AddressTypeIPv4
					} else {
						eps.AddressType = discovery.AddressTypeIPv6
					}
					for _, ip := range tc.epIPs {
						eps.Endpoints = append(eps.Endpoints, discovery.Endpoint{
							Addresses: []string{ip},
							NodeName:  new(testNodeName),
						})
					}
					eps.Ports = []discovery.EndpointPort{{
						Name:     new("p80"),
						Port:     new(int32(80)),
						Protocol: new(v1.ProtocolTCP),
					}}
				}),
			)

			if err := fp.syncProxyRules(); err != nil {
				t.Fatalf("syncProxyRules failed: %v", err)
			}

			svcChain := "service-ULMVA6XW-ns1/svc1/tcp/p80"
			rules, err := nft.ListRules(context.Background(), svcChain)
			if err != nil {
				t.Fatalf("could not list rules in %s: %v", svcChain, err)
			}
			if len(rules) != 1 {
				t.Fatalf("expected exactly 1 rule in %s, got %d", svcChain, len(rules))
			}
			if rules[0].Rule != tc.expectedRule {
				t.Errorf("wrong DNAT rule:\nexpected %q\ngot      %q", tc.expectedRule, rules[0].Rule)
			}
		})
	}
}

// TestTruncateNFTablesComment 验证 truncateNFTablesComment 对短注释原样保留，
// 对超长注释截断到 nftables 允许的最大长度。
func TestTruncateNFTablesComment(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "short comment unchanged",
			input: "kube-system/kube-dns:dns",
			want:  "kube-system/kube-dns:dns",
		},
		{
			name:  "exactly max length unchanged",
			input: strings.Repeat("a", knftables.CommentLengthMax),
			want:  strings.Repeat("a", knftables.CommentLengthMax),
		},
		{
			name:  "too long gets truncated",
			input: strings.Repeat("a", knftables.CommentLengthMax+1),
			want:  strings.Repeat("a", knftables.CommentLengthMax),
		},
		{
			name:  "realistic long service name truncated",
			input: "loft-acme-platform-services-team-dev-v-shared-workloads-ns-001/my-api-svc-x-my-api-namespace-x-acme-shared-vcluster--a1b2c3d4e:http",
			want:  "loft-acme-platform-services-team-dev-v-shared-workloads-ns-001/my-api-svc-x-my-api-namespace-x-acme-shared-vcluster--a1b2c3d4e:h",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateNFTablesComment(tc.input)
			if got != tc.want {
				t.Errorf("truncateNFTablesComment(%q) = %q, want %q", tc.input, got, tc.want)
			}
			if len(got) > knftables.CommentLengthMax {
				t.Errorf("result length %d exceeds max %d", len(got), knftables.CommentLengthMax)
			}
		})
	}
}

// TestSyncProxyRulesTruncatesNoEndpointServiceComment 验证无端点 Service 的超长注释
// 在写入 no-endpoint-services map 时会被截断，且不会把完整注释写进规则。
func TestSyncProxyRulesTruncatesNoEndpointServiceComment(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	serviceNamespace := "loft-acme-platform-services-team-dev-v-shared-workloads-ns-001"
	serviceName := "my-api-svc-x-my-api-namespace-x-acme-shared-vcluster--a1b2c3d4e"
	portName := "http"
	serviceComment := fmt.Sprintf("%s/%s:%s", serviceNamespace, serviceName, portName)
	truncatedComment := truncateNFTablesComment(serviceComment)
	if len(serviceComment) <= knftables.CommentLengthMax {
		t.Fatalf("test service comment length %d should exceed max %d", len(serviceComment), knftables.CommentLengthMax)
	}

	makeServiceMap(fp,
		makeTestService(serviceNamespace, serviceName, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.47"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     portName,
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	if err := fp.syncProxyRules(); err != nil {
		t.Fatalf("syncProxyRules failed: %v", err)
	}

	expected := truncatedComment
	elem := nft.Table.Maps["no-endpoint-services"].FindElement("172.30.0.47", "tcp", "80")
	if elem == nil || elem.Comment == nil || *elem.Comment != expected {
		t.Fatalf("expected nftables transaction to contain truncated comment %q, got: %+v", expected, elem)
	}
	if strings.Contains(nft.Dump(), serviceComment) {
		t.Fatalf("nftables transaction contains untruncated comment %q", serviceComment)
	}
}

// TestNoEndpointsReject 验证没有端点的 Service 会拒绝发往其 ClusterIP、ExternalIP、
// NodePort 和 LoadBalancer IP 的连接。
func TestNoEndpointsReject(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcExternalIPs := "192.168.99.11"
	svcLBIP := "1.2.3.4"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Protocol: v1.ProtocolTCP,
				Port:     int32(svcPort),
				NodePort: int32(svcNodePort),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: svcLBIP,
			}}
		}),
	)
	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP with no endpoints",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   "REJECT",
		},
		{
			name:     "external to external IP with no endpoints",
			sourceIP: testExternalClient,
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   "REJECT",
		},
		{
			name:     "pod to NodePort with no endpoints",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   "REJECT",
		},
		{
			name:     "external to NodePort with no endpoints",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   "REJECT",
		},
		{
			name:     "pod to LoadBalancer IP with no endpoints",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   "REJECT",
		},
		{
			name:     "external to LoadBalancer IP with no endpoints",
			sourceIP: testExternalClient,
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   "REJECT",
		},
	})
}

// TestClusterIPGeneral 覆盖 ClusterIP Service 的若干基本特性
func TestClusterIPGeneral(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "http",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{
				{
					Name:     "http",
					Port:     80,
					Protocol: v1.ProtocolTCP,
				},
				{
					Name:       "https",
					Port:       443,
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt32(8443),
				},
				{
					Name:     "dns-udp",
					Port:     53,
					Protocol: v1.ProtocolUDP,
				},
				{
					Name:     "dns-tcp",
					Port:     53,
					Protocol: v1.ProtocolTCP,
					// 只在 TCP 端口上设置 TargetPort，UDP/SCTP 不设置，
					// 这样输出中的端口映射就能明确区分协议，避免断言歧义。
					TargetPort: intstr.FromInt32(5353),
				},
				{
					Name:     "dns-sctp",
					Port:     53,
					Protocol: v1.ProtocolSCTP,
				},
			}
		}),
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("http"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{"10.180.0.1"},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{"10.180.2.1"},
					NodeName:  ptr.To("node2"),
				},
			}
			eps.Ports = []discovery.EndpointPort{
				{
					Name:     ptr.To("http"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				},
				{
					Name:     ptr.To("https"),
					Port:     ptr.To[int32](8443),
					Protocol: ptr.To(v1.ProtocolTCP),
				},
				{
					Name:     ptr.To("dns-udp"),
					Port:     ptr.To[int32](53),
					Protocol: ptr.To(v1.ProtocolUDP),
				},
				{
					Name:     ptr.To("dns-tcp"),
					Port:     ptr.To[int32](5353),
					Protocol: ptr.To(v1.ProtocolTCP),
				},
				{
					Name:     ptr.To("dns-sctp"),
					Port:     ptr.To[int32](53),
					Protocol: ptr.To(v1.ProtocolSCTP),
				},
			}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		{
			name:     "simple clusterIP",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80",
			masq:     false,
		},
		{
			name:     "hairpin to cluster IP",
			sourceIP: "10.180.0.1",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80",
			masq:     true,
		},
		{
			name:     "clusterIP with multiple endpoints",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.42",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.2.1:80",
			masq:     false,
		},
		{
			name:     "clusterIP with TargetPort",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.42",
			destPort: 443,
			output:   "10.180.0.1:8443, 10.180.2.1:8443",
			masq:     false,
		},
		{
			name:     "clusterIP with TCP, UDP, and SCTP on same port (TCP)",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolTCP,
			destIP:   "172.30.0.42",
			destPort: 53,
			output:   "10.180.0.1:5353, 10.180.2.1:5353",
			masq:     false,
		},
		{
			name:     "clusterIP with TCP, UDP, and SCTP on same port (TCP)",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolUDP,
			destIP:   "172.30.0.42",
			destPort: 53,
			output:   "10.180.0.1:53, 10.180.2.1:53",
			masq:     false,
		},
		{
			name:     "clusterIP with TCP, UDP, and SCTP on same port (SCTP)",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolSCTP,
			destIP:   "172.30.0.42",
			destPort: 53,
			output:   "10.180.0.1:53, 10.180.2.1:53",
			masq:     false,
		},
		{
			name:     "TCP-only port does not match UDP traffic",
			sourceIP: "10.180.0.2",
			protocol: v1.ProtocolUDP,
			destIP:   "172.30.0.42",
			destPort: 80,
			output:   "REJECT",
		},
		{
			name:     "svc1 does not accept svc2's ports",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.41",
			destPort: 443,
			output:   "REJECT",
		},
		{
			name:     "packet to unallocated cluster ip",
			sourceIP: "10.180.0.2",
			destIP:   "172.30.0.50",
			destPort: 80,
			output:   "DROP",
		},
	})
}

// TestLoadBalancer 覆盖带 LoadBalancerSourceRanges 的 LoadBalancer Service：
// 校验 ClusterIP、NodePort 与各 LB IP 的转发路径及源地址过滤。
func TestLoadBalancer(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcLBIP1 := "1.2.3.4"
	svcLBIP2 := "5.6.7.8"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{
				{IP: svcLBIP1},
				{IP: svcLBIP2},
			}
			svc.Spec.LoadBalancerSourceRanges = []string{
				"192.168.0.0/24",

				// 回归测试：多余的空白应被忽略
				" 203.0.113.0/25",
			}
		}),
	)

	epIP := "10.180.0.1"
	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIP},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     false,
		},
		{
			name:     "external to nodePort",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "nodePort bypasses LoadBalancerSourceRanges",
			sourceIP: testExternalClientBlocked,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "accepted external to LB1",
			sourceIP: testExternalClient,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "accepted external to LB2",
			sourceIP: testExternalClient,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "blocked external to LB1",
			sourceIP: testExternalClientBlocked,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "blocked external to LB2",
			sourceIP: testExternalClientBlocked,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "pod to LB1 (blocked by LoadBalancerSourceRanges)",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "pod to LB2 (blocked by LoadBalancerSourceRanges)",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   "DROP",
		},
		{
			name:     "node to LB1 (allowed by LoadBalancerSourceRanges)",
			sourceIP: testNodeIP,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "node to LB2 (allowed by LoadBalancerSourceRanges)",
			sourceIP: testNodeIP,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},

		// LB 规则假定从节点访问 LB IP 时，会有 kube-proxy 之外的机制把连接
		// SNAT 成 LB IP；因此若 LoadBalancerSourceRanges 包含节点 IP，
		// 还需要额外放行来自 LB IP 的流量。
		{
			name:     "same node to LB1, SNATted to LB1 (implicitly allowed)",
			sourceIP: svcLBIP1,
			destIP:   svcLBIP1,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
		{
			name:     "same node to LB2, SNATted to LB2 (implicitly allowed)",
			sourceIP: svcLBIP2,
			destIP:   svcLBIP2,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP, svcPort),
			masq:     true,
		},
	})
}

// TestNodePorts 在不同 --nodeport-addresses 配置组合下测试 NodePort Service。
func TestNodePorts(t *testing.T) {
	testCases := []struct {
		name string

		family            v1.IPFamily
		nodePortAddresses []string

		// allowAltNodeIP 表示预期备用节点 IP 上的 NodePort 流量会被接受
		allowAltNodeIP bool
	}{
		{
			name: "ipv4",

			family:            v1.IPv4Protocol,
			nodePortAddresses: nil,

			allowAltNodeIP: false,
		},
		{
			name: "ipv4, multiple nodeport-addresses",

			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"192.168.0.0/24", "192.168.1.0/24", "2001:db8::/64"},

			allowAltNodeIP: true,
		},
		{
			name: "ipv6",

			family:            v1.IPv6Protocol,
			nodePortAddresses: nil,

			allowAltNodeIP: false,
		},
		{
			name: "ipv6, multiple nodeport-addresses",

			family:            v1.IPv6Protocol,
			nodePortAddresses: []string{"192.168.0.0/24", "192.168.1.0/24", "2001:db8::/64", "2001:db8:1::2/128"},

			allowAltNodeIP: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nft, fp := NewFakeProxier(tc.family)

			var svcIP, epIP1, epIP2 string
			var nodeIP string
			if tc.family == v1.IPv4Protocol {
				svcIP = "172.30.0.41"
				epIP1 = "10.180.0.1"
				epIP2 = "10.180.2.1"
				nodeIP = testNodeIP
			} else {
				svcIP = "fd00:172:30::41"
				epIP1 = "fd00:10:180::1"
				epIP2 = "fd00:10:180::2:1"
				nodeIP = testNodeIPv6
			}
			if tc.nodePortAddresses != nil {
				fp.nodePortAddresses = proxyutil.NewNodePortAddresses(tc.family, tc.nodePortAddresses)
			}

			makeServiceMap(fp,
				makeTestService("ns1", "svc1", func(svc *v1.Service) {
					svc.Spec.Type = v1.ServiceTypeNodePort
					svc.Spec.ClusterIP = svcIP
					svc.Spec.Ports = []v1.ServicePort{{
						Name:     "p80",
						Port:     80,
						Protocol: v1.ProtocolTCP,
						NodePort: 3001,
					}}
				}),
			)

			populateEndpointSlices(fp,
				makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
					if tc.family == v1.IPv4Protocol {
						eps.AddressType = discovery.AddressTypeIPv4
					} else {
						eps.AddressType = discovery.AddressTypeIPv6
					}
					eps.Endpoints = []discovery.Endpoint{{
						Addresses: []string{epIP1},
						NodeName:  ptr.To("other-node"),
					}, {
						Addresses: []string{epIP2},
						NodeName:  ptr.To(testNodeName),
					}}
					eps.Ports = []discovery.EndpointPort{{
						Name:     ptr.To("p80"),
						Port:     ptr.To[int32](80),
						Protocol: ptr.To(v1.ProtocolTCP),
					}}
				}),
			)

			fp.syncProxyRules()

			var podIP, externalClientIP, altNodeIP string
			if tc.family == v1.IPv4Protocol {
				podIP = "10.0.0.2"
				externalClientIP = testExternalClient
				altNodeIP = testNodeIPAlt
			} else {
				podIP = "fd00:10::2"
				externalClientIP = "2600:5200::1"
				altNodeIP = testNodeIPv6Alt
			}
			output := net.JoinHostPort(epIP1, "80") + ", " + net.JoinHostPort(epIP2, "80")

			// 各场景的基础用例相同
			runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
				{
					name:     "pod to cluster IP",
					sourceIP: podIP,
					destIP:   svcIP,
					destPort: 80,
					output:   output,
					masq:     false,
				},
				{
					name:     "external to nodePort",
					sourceIP: externalClientIP,
					destIP:   nodeIP,
					destPort: 3001,
					output:   output,
					masq:     true,
				},
				{
					name:     "node to nodePort",
					sourceIP: nodeIP,
					destIP:   nodeIP,
					destPort: 3001,
					output:   output,
					masq:     true,
				},
			})

			if tc.allowAltNodeIP {
				runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
					{
						name:     "external to nodePort on secondary IP",
						sourceIP: externalClientIP,
						destIP:   altNodeIP,
						destPort: 3001,
						output:   output,
						masq:     true,
					},
				})
			} else {
				runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
					{
						name:     "secondary nodeIP ignores NodePorts",
						sourceIP: externalClientIP,
						destIP:   altNodeIP,
						destPort: 3001,
						output:   "",
					},
				})
			}
		})
	}
}

// TestExternalTrafficPolicyLocal 验证使用 Local 策略时，发往外部 IP 的流量不会被伪装。
// 对外部来源的流量，这意味着只能路由到本地端点；
// 而对集群内部来源的流量，仍会路由到全部端点。
func TestExternalTrafficPolicyLocal(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)

	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcHealthCheckNodePort := 30000
	svcExternalIPs := "192.168.99.11"
	svcLBIP := "1.2.3.4"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       svcPortName.Port,
				Port:       int32(svcPort),
				Protocol:   v1.ProtocolTCP,
				NodePort:   int32(svcNodePort),
				TargetPort: intstr.FromInt32(int32(svcPort)),
			}}
			svc.Spec.HealthCheckNodePort = int32(svcHealthCheckNodePort)
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: svcLBIP,
			}}
		}),
	)

	epIP1 := "10.180.0.1"
	epIP2 := "10.180.2.1"
	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIP1},
				NodeName:  ptr.To("other-node"),
			}, {
				Addresses: []string{epIP2},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to external IP hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "external to external IP hits only local endpoint, unmasqueraded",
			sourceIP: testExternalClient,
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to LB IP hits only both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "external to LB IP hits only local endpoint, unmasqueraded",
			sourceIP: testExternalClient,
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d", epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to NodePort hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "external to NodePort hits only local endpoint, unmasqueraded",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d", epIP2, svcPort),
			masq:     false,
		},
	})
}

// TestExternalTrafficPolicyCluster 验证使用 Cluster 策略时，发往外部 IP 的流量会被伪装。
func TestExternalTrafficPolicyCluster(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)

	svcIP := "172.30.0.41"
	svcPort := 80
	svcNodePort := 3001
	svcExternalIPs := "192.168.99.11"
	svcLBIP := "1.2.3.4"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
	}

	makeServiceMap(fp,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:       svcPortName.Port,
				Port:       int32(svcPort),
				Protocol:   v1.ProtocolTCP,
				NodePort:   int32(svcNodePort),
				TargetPort: intstr.FromInt32(int32(svcPort)),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: svcLBIP,
			}}
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
		}),
	)

	epIP1 := "10.180.0.1"
	epIP2 := "10.180.2.1"
	populateEndpointSlices(fp,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIP1},
				NodeName:  ptr.To("other-node"),
			}, {
				Addresses: []string{epIP2},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	runPacketFlowTests(t, getLine(), nft, testNodeIPs, []packetFlowTest{
		{
			name:     "pod to cluster IP hits both endpoints, unmasqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     false,
		},
		{
			name:     "pod to external IP hits both endpoints, masqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "external to external IP hits both endpoints, masqueraded",
			sourceIP: testExternalClient,
			destIP:   svcExternalIPs,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "pod to LB IP hits both endpoints, masqueraded",
			sourceIP: "10.0.0.2",
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "external to LB IP hits both endpoints, masqueraded",
			sourceIP: testExternalClient,
			destIP:   svcLBIP,
			destPort: svcPort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "pod to NodePort hits both endpoints, masqueraded",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
		{
			name:     "external to NodePort hits both endpoints, masqueraded",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: svcNodePort,
			output:   fmt.Sprintf("%s:%d, %s:%d", epIP1, svcPort, epIP2, svcPort),
			masq:     true,
		},
	})
}

// makeTestService 构造一个带命名空间/名称的 Service，并交给调用方回调填充 Spec。
func makeTestService(namespace, name string, svcFunc func(*v1.Service)) *v1.Service {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{},
		},
		Spec:   v1.ServiceSpec{},
		Status: v1.ServiceStatus{},
	}
	svcFunc(svc)
	return svc
}

// addTestPort 向端口切片追加一个 ServicePort，并设置协议、端口、NodePort 与 TargetPort。
func addTestPort(array []v1.ServicePort, name string, protocol v1.Protocol, port, nodeport int32, targetPort int) []v1.ServicePort {
	svcPort := v1.ServicePort{
		Name:       name,
		Protocol:   protocol,
		Port:       port,
		NodePort:   nodeport,
		TargetPort: intstr.FromInt32(int32(targetPort)),
	}
	return append(array, svcPort)
}

// TestBuildServiceMapAddRemove 验证 Service 的增删与端口变更会正确反映到 svcPortMap，
// 包含 Local 策略 LoadBalancer 的健康检查端口的增删。
func TestBuildServiceMapAddRemove(t *testing.T) {
	_, fp := NewFakeProxier(v1.IPv4Protocol)

	services := []*v1.Service{
		makeTestService("somewhere-else", "cluster-ip", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.55.4"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "something", "UDP", 1234, 4321, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "UDP", 1235, 5321, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "sctpport", "SCTP", 1236, 6321, 0)
		}),
		makeTestService("somewhere-else", "node-port", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.55.10"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "blahblah", "UDP", 345, 678, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "moreblahblah", "TCP", 344, 677, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "muchmoreblah", "SCTP", 343, 676, 0)
		}),
		makeTestService("somewhere", "load-balancer", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = "172.30.55.11"
			svc.Spec.LoadBalancerIP = "1.2.3.4"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "foobar", "UDP", 8675, 30061, 7000)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "baz", "UDP", 8676, 30062, 7001)
			svc.Status.LoadBalancer = v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			}
		}),
		makeTestService("somewhere", "only-local-load-balancer", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = "172.30.55.12"
			svc.Spec.LoadBalancerIP = "5.6.7.8"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "foobar2", "UDP", 8677, 30063, 7002)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "baz", "UDP", 8678, 30064, 7003)
			svc.Status.LoadBalancer = v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "5.6.7.8"},
				},
			}
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.HealthCheckNodePort = 345
		}),
	}

	for i := range services {
		fp.OnServiceAdd(services[i])
	}
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 10 {
		t.Errorf("expected service map length 10, got %v", fp.svcPortMap)
	}

	// only-local-load-balancer 的健康检查端口会被加入
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected 1 healthcheck port, got %v", healthCheckNodePorts)
	} else {
		nsn := makeNSN("somewhere", "only-local-load-balancer")
		if port, found := healthCheckNodePorts[nsn]; !found || port != 345 {
			t.Errorf("expected healthcheck port [%q]=345: got %v", nsn, healthCheckNodePorts)
		}
	}

	// 删除部分 Service；oneService 由 services[0] 修改而来，去掉了第一个端口。
	oneService := makeTestService("somewhere-else", "cluster-ip", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.30.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "UDP", 1235, 5321, 0)
	})

	fp.OnServiceUpdate(services[0], oneService)
	fp.OnServiceDelete(services[1])
	fp.OnServiceDelete(services[2])
	fp.OnServiceDelete(services[3])

	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 1 {
		t.Errorf("expected service map length 1, got %v", fp.svcPortMap)
	}

	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected 0 healthcheck ports, got %v", healthCheckNodePorts)
	}
}

// TestBuildServiceMapServiceHeadless 验证 headless Service（含无端口的）不会进入 svcPortMap。
func TestBuildServiceMapServiceHeadless(t *testing.T) {
	_, fp := NewFakeProxier(v1.IPv4Protocol)

	makeServiceMap(fp,
		makeTestService("somewhere-else", "headless", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = v1.ClusterIPNone
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "rpc", "UDP", 1234, 0, 0)
		}),
		makeTestService("somewhere-else", "headless-without-port", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = v1.ClusterIPNone
		}),
	)

	// headless Service 应被忽略
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 0 {
		t.Errorf("expected service map length 0, got %d", len(fp.svcPortMap))
	}

	// 没有被代理的 Service，因此不应有健康检查端口
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %d", len(healthCheckNodePorts))
	}
}

// TestBuildServiceMapServiceTypeExternalName 验证 ExternalName 类型的 Service 会被忽略。
func TestBuildServiceMapServiceTypeExternalName(t *testing.T) {
	_, fp := NewFakeProxier(v1.IPv4Protocol)

	makeServiceMap(fp,
		makeTestService("somewhere-else", "external-name", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeExternalName
			svc.Spec.ClusterIP = "172.30.55.4" // 应被忽略
			svc.Spec.ExternalName = "foo2.bar.com"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "blah", "UDP", 1235, 5321, 0)
		}),
	)

	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 0 {
		t.Errorf("expected service map length 0, got %v", fp.svcPortMap)
	}
	// 没有被代理的 Service，因此不应有健康检查端口
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}
}

// TestBuildServiceMapServiceUpdate 验证 Service 在 ClusterIP 与 LoadBalancer 之间转换及无变化更新时，
// svcPortMap 与健康检查端口均能正确更新。
func TestBuildServiceMapServiceUpdate(t *testing.T) {
	_, fp := NewFakeProxier(v1.IPv4Protocol)

	servicev1 := makeTestService("somewhere", "some-service", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.30.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "something", "UDP", 1234, 4321, 0)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "TCP", 1235, 5321, 0)
	})
	servicev2 := makeTestService("somewhere", "some-service", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeLoadBalancer
		svc.Spec.ClusterIP = "172.30.55.4"
		svc.Spec.LoadBalancerIP = "1.2.3.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "something", "UDP", 1234, 4321, 7002)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "somethingelse", "TCP", 1235, 5321, 7003)
		svc.Status.LoadBalancer = v1.LoadBalancerStatus{
			Ingress: []v1.LoadBalancerIngress{
				{IP: "1.2.3.4"},
			},
		}
		svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		svc.Spec.HealthCheckNodePort = 345
	})

	fp.OnServiceAdd(servicev1)

	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}

	// 将 Service 改为 LoadBalancer 类型
	fp.OnServiceUpdate(servicev1, servicev2)
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected healthcheck ports length 1, got %v", healthCheckNodePorts)
	}

	// 无变化时，Service map 应保持不变，健康检查配置也不应变动
	fp.OnServiceUpdate(servicev2, servicev2)
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected healthcheck ports length 1, got %v", healthCheckNodePorts)
	}

	// 再改回 ClusterIP 类型
	fp.OnServiceUpdate(servicev2, servicev1)
	fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}
}

// populateEndpointSlices 把给定的 EndpointSlice 依次加入 Proxier 的端点变更跟踪器。
func populateEndpointSlices(proxier *Proxier, allEndpointSlices ...*discovery.EndpointSlice) {
	for i := range allEndpointSlices {
		proxier.OnEndpointSliceAdd(allEndpointSlices[i])
	}
}

// makeTestEndpointSlice 构造一个带服务标签和编号的 EndpointSlice，细节由回调填充。
func makeTestEndpointSlice(namespace, name string, sliceNum int, epsFunc func(*discovery.EndpointSlice)) *discovery.EndpointSlice {
	eps := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", name, sliceNum),
			Namespace: namespace,
			Labels:    map[string]string{discovery.LabelServiceName: name},
		},
	}
	epsFunc(eps)
	return eps
}

// makeNSN 由命名空间和名称构造 NamespacedName。
func makeNSN(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// makeServicePortName 由命名空间、服务名、端口名和协议构造 ServicePortName。
func makeServicePortName(ns, name, port string, protocol v1.Protocol) proxy.ServicePortName {
	return proxy.ServicePortName{
		NamespacedName: makeNSN(ns, name),
		Port:           port,
		Protocol:       protocol,
	}
}

// makeServiceMap 批量加入 Service，并标记服务已同步，避免首次同步被视为未初始化。
func makeServiceMap(proxier *Proxier, allServices ...*v1.Service) {
	for i := range allServices {
		proxier.OnServiceAdd(allServices[i])
	}

	proxier.mu.Lock()
	defer proxier.mu.Unlock()
	proxier.servicesSynced = true
}

type endpointExpectation struct {
	endpoint string
	isLocal  bool
}

// checkEndpointExpectations 比较实际端点映射与预期端点的地址和本地性，并逐项报错。
func checkEndpointExpectations(t *testing.T, tci int, newMap proxy.EndpointsMap, expected map[proxy.ServicePortName][]endpointExpectation) {
	if len(newMap) != len(expected) {
		t.Errorf("[%d] expected %d results, got %d: %v", tci, len(expected), len(newMap), newMap)
	}
	for x := range expected {
		if len(newMap[x]) != len(expected[x]) {
			t.Errorf("[%d] expected %d endpoints for %v, got %d", tci, len(expected[x]), x, len(newMap[x]))
		} else {
			for i := range expected[x] {
				newEp := newMap[x][i]
				if newEp.String() != expected[x][i].endpoint ||
					newEp.IsLocal() != expected[x][i].isLocal {
					t.Errorf("[%d] expected new[%v][%d] to be %v, got %v", tci, x, i, expected[x][i], newEp)
				}
			}
		}
	}
}

// TestUpdateEndpointsMap 以多组“前/后”EndpointSlice 验证端点映射的增删改、
// 命名端口变化及本地性判定。
func TestUpdateEndpointsMap(t *testing.T) {
	emptyEndpointSlices := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, func(*discovery.EndpointSlice) {}),
	}
	subset1 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
			NodeName:  ptr.To("other-node"),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subset2 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.2"},
			NodeName:  ptr.To("other-node"),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPortLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
					NodeName:  ptr.To(testNodeName),
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11"),
					Port:     ptr.To[int32](11),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	namedPort := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
	}
	namedPortRenamed := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
					NodeName:  ptr.To("other-node"),
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11-2"),
					Port:     ptr.To[int32](11),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	namedPortRenumbered := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
					NodeName:  ptr.To("other-node"),
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11"),
					Port:     ptr.To[int32](22),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	namedPortsLocalNoLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1,
			func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{{
					Addresses: []string{"10.1.1.1"},
					NodeName:  ptr.To("other-node"),
				}, {
					Addresses: []string{"10.1.1.2"},
					NodeName:  ptr.To(testNodeName),
				}}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p11"),
					Port:     ptr.To[int32](11),
					Protocol: ptr.To(v1.ProtocolUDP),
				}, {
					Name:     ptr.To("p12"),
					Port:     ptr.To[int32](12),
					Protocol: ptr.To(v1.ProtocolUDP),
				}}
			}),
	}
	multipleSubsets := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
		makeTestEndpointSlice("ns1", "ep1", 2, subset2),
	}
	subsetLocal := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.2"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsWithLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
		makeTestEndpointSlice("ns1", "ep1", 2, subsetLocal),
	}
	subsetMultiplePortsLocal := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subset3 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.3"},
			NodeName:  ptr.To("other-node"),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p13"),
			Port:     ptr.To[int32](13),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsMultiplePortsLocal := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subsetMultiplePortsLocal),
		makeTestEndpointSlice("ns1", "ep1", 2, subset3),
	}
	subsetMultipleIPsPorts1 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
			NodeName:  ptr.To("other-node"),
		}, {
			Addresses: []string{"10.1.1.2"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subsetMultipleIPsPorts2 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.3"},
			NodeName:  ptr.To("other-node"),
		}, {
			Addresses: []string{"10.1.1.4"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p13"),
			Port:     ptr.To[int32](13),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p14"),
			Port:     ptr.To[int32](14),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	subsetMultipleIPsPorts3 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.2.2.1"},
			NodeName:  ptr.To("other-node"),
		}, {
			Addresses: []string{"10.2.2.2"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p21"),
			Port:     ptr.To[int32](21),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p22"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsIPsPorts := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subsetMultipleIPsPorts1),
		makeTestEndpointSlice("ns1", "ep1", 2, subsetMultipleIPsPorts2),
		makeTestEndpointSlice("ns2", "ep2", 1, subsetMultipleIPsPorts3),
	}
	complexSubset1 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.2.2.2"},
			NodeName:  ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.2.2.22"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p22"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset2 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.2.2.3"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p23"),
			Port:     ptr.To[int32](23),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset3 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.4.4"},
			NodeName:  ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.4.4.5"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p44"),
			Port:     ptr.To[int32](44),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset4 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.4.6"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p45"),
			Port:     ptr.To[int32](45),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset5 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.1"},
			NodeName:  ptr.To("other-node"),
		}, {
			Addresses: []string{"10.1.1.11"},
			NodeName:  ptr.To("other-node"),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset6 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.1.1.2"},
			NodeName:  ptr.To("other-node"),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p122"),
			Port:     ptr.To[int32](122),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset7 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.3.3.3"},
			NodeName:  ptr.To("other-node"),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p33"),
			Port:     ptr.To[int32](33),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexSubset8 := func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"10.4.4.4"},
			NodeName:  ptr.To(testNodeName),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p44"),
			Port:     ptr.To[int32](44),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, subset1),
		nil,
		makeTestEndpointSlice("ns2", "ep2", 1, complexSubset1),
		makeTestEndpointSlice("ns2", "ep2", 2, complexSubset2),
		nil,
		makeTestEndpointSlice("ns4", "ep4", 1, complexSubset3),
		makeTestEndpointSlice("ns4", "ep4", 2, complexSubset4),
	}
	complexAfter := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "ep1", 1, complexSubset5),
		makeTestEndpointSlice("ns1", "ep1", 2, complexSubset6),
		nil,
		nil,
		makeTestEndpointSlice("ns3", "ep3", 1, complexSubset7),
		makeTestEndpointSlice("ns4", "ep4", 1, complexSubset8),
		nil,
	}

	testCases := []struct {
		// previousEndpoints 与 currentEndpoints 用于决定调用哪个 OnEndpoints* 回调
		// （取决于对应值是否为 nil），因此两者长度必须一致。
		name                             string
		previousEndpoints                []*discovery.EndpointSlice
		currentEndpoints                 []*discovery.EndpointSlice
		oldEndpoints                     map[proxy.ServicePortName][]endpointExpectation
		expectedResult                   map[proxy.ServicePortName][]endpointExpectation
		expectedConntrackCleanupRequired bool
		expectedLocalEndpoints           map[types.NamespacedName]int
	}{{
		// Case[0]：无变化
		name:                             "nothing",
		oldEndpoints:                     map[proxy.ServicePortName][]endpointExpectation{},
		expectedResult:                   map[proxy.ServicePortName][]endpointExpectation{},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[1]：无变化，命名端口，本地端点
		name:              "no change, named port, local",
		previousEndpoints: namedPortLocal,
		currentEndpoints:  namedPortLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[2]：无变化，多个子集
		name:              "no change, multiple subsets",
		previousEndpoints: multipleSubsets,
		currentEndpoints:  multipleSubsets,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[3]：无变化，多个子集、多个端口，本地端点
		name:              "no change, multiple subsets, multiple ports, local",
		previousEndpoints: multipleSubsetsMultiplePortsLocal,
		currentEndpoints:  multipleSubsetsMultiplePortsLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[4]：无变化，多个端点、子集、IP 和端口
		name:              "no change, multiple endpoints, subsets, IPs, and ports",
		previousEndpoints: multipleSubsetsIPsPorts,
		currentEndpoints:  multipleSubsetsIPsPorts,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
				{endpoint: "10.1.1.4:13", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p14", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:14", isLocal: false},
				{endpoint: "10.1.1.4:14", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p21", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:21", isLocal: false},
				{endpoint: "10.2.2.2:21", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:22", isLocal: false},
				{endpoint: "10.2.2.2:22", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:13", isLocal: false},
				{endpoint: "10.1.1.4:13", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p14", v1.ProtocolUDP): {
				{endpoint: "10.1.1.3:14", isLocal: false},
				{endpoint: "10.1.1.4:14", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p21", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:21", isLocal: false},
				{endpoint: "10.2.2.2:21", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{endpoint: "10.2.2.1:22", isLocal: false},
				{endpoint: "10.2.2.2:22", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 2,
			makeNSN("ns2", "ep2"): 1,
		},
	}, {
		// Case[5]：新增一个 Endpoints
		name:              "add an Endpoints",
		previousEndpoints: []*discovery.EndpointSlice{nil},
		currentEndpoints:  namedPortLocal,
		oldEndpoints:      map[proxy.ServicePortName][]endpointExpectation{},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[6]：删除一个 Endpoints
		name:              "remove an Endpoints",
		previousEndpoints: namedPortLocal,
		currentEndpoints:  []*discovery.EndpointSlice{nil},
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[7]：新增一个 IP 和端口
		name:              "add an IP and port",
		previousEndpoints: namedPort,
		currentEndpoints:  namedPortsLocalNoLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[8]：删除一个 IP 和端口
		name:              "remove an IP and port",
		previousEndpoints: namedPortsLocalNoLocal,
		currentEndpoints:  namedPort,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
				{endpoint: "10.1.1.2:11", isLocal: true},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:12", isLocal: false},
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[9]：新增一个子集
		name:              "add a subset",
		previousEndpoints: []*discovery.EndpointSlice{namedPort[0], nil},
		currentEndpoints:  multipleSubsetsWithLocal,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
	}, {
		// Case[10]：删除一个子集
		name:              "remove a subset",
		previousEndpoints: multipleSubsets,
		currentEndpoints:  []*discovery.EndpointSlice{namedPort[0], nil},
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[11]：重命名端口
		name:              "rename a port",
		previousEndpoints: namedPort,
		currentEndpoints:  namedPortRenamed,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11-2", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[12]：修改端口号
		name:              "renumber a port",
		previousEndpoints: namedPort,
		currentEndpoints:  namedPortRenumbered,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:22", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	}, {
		// Case[13]：复杂的增删组合
		name:              "complex add and remove",
		previousEndpoints: complexBefore,
		currentEndpoints:  complexAfter,
		oldEndpoints: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{endpoint: "10.2.2.22:22", isLocal: true},
				{endpoint: "10.2.2.2:22", isLocal: true},
			},
			makeServicePortName("ns2", "ep2", "p23", v1.ProtocolUDP): {
				{endpoint: "10.2.2.3:23", isLocal: true},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolUDP): {
				{endpoint: "10.4.4.4:44", isLocal: true},
				{endpoint: "10.4.4.5:44", isLocal: true},
			},
			makeServicePortName("ns4", "ep4", "p45", v1.ProtocolUDP): {
				{endpoint: "10.4.4.6:45", isLocal: true},
			},
		},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.11:11", isLocal: false},
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:12", isLocal: false},
			},
			makeServicePortName("ns1", "ep1", "p122", v1.ProtocolUDP): {
				{endpoint: "10.1.1.2:122", isLocal: false},
			},
			makeServicePortName("ns3", "ep3", "p33", v1.ProtocolUDP): {
				{endpoint: "10.3.3.3:33", isLocal: false},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolUDP): {
				{endpoint: "10.4.4.4:44", isLocal: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns4", "ep4"): 1,
		},
	}, {
		// Case[14]：从 0 个端点地址变为 1 个未命名端口
		name:              "change from 0 endpoint address to 1 unnamed port",
		previousEndpoints: emptyEndpointSlices,
		currentEndpoints:  namedPort,
		oldEndpoints:      map[proxy.ServicePortName][]endpointExpectation{},
		expectedResult: map[proxy.ServicePortName][]endpointExpectation{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{endpoint: "10.1.1.1:11", isLocal: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
	},
	}

	for tci, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, fp := NewFakeProxier(v1.IPv4Protocol)

			// 首先确认：加入所有历史版本的端点后，fp.oldEndpoints 与预期一致。
			for i := range tc.previousEndpoints {
				if tc.previousEndpoints[i] != nil {
					fp.OnEndpointSliceAdd(tc.previousEndpoints[i])
				}
			}
			fp.endpointsMap.Update(fp.endpointsChanges)
			checkEndpointExpectations(t, tci, fp.endpointsMap, tc.oldEndpoints)

			// 调用相应的回调，把状态切换到各用例期望的结果。
			if len(tc.previousEndpoints) != len(tc.currentEndpoints) {
				t.Fatalf("[%d] different lengths of previous and current endpoints", tci)
			}

			for i := range tc.previousEndpoints {
				prev, curr := tc.previousEndpoints[i], tc.currentEndpoints[i]
				switch {
				case prev == nil:
					fp.OnEndpointSliceAdd(curr)
				case curr == nil:
					fp.OnEndpointSliceDelete(prev)
				default:
					fp.OnEndpointSliceUpdate(prev, curr)
				}
			}
			result := fp.endpointsMap.Update(fp.endpointsChanges)
			newMap := fp.endpointsMap
			checkEndpointExpectations(t, tci, newMap, tc.expectedResult)
			if result.ConntrackCleanupRequired != tc.expectedConntrackCleanupRequired {
				t.Errorf("[%d] expected conntrackCleanupRequired to be %t, got %t", tci, tc.expectedConntrackCleanupRequired, result.ConntrackCleanupRequired)
			}
			localReadyEndpoints := fp.endpointsMap.LocalReadyEndpoints()
			if !reflect.DeepEqual(localReadyEndpoints, tc.expectedLocalEndpoints) {
				t.Errorf("[%d] expected local endpoints %v, got %v", tci, tc.expectedLocalEndpoints, localReadyEndpoints)
			}
		})
	}
}

// TestHealthCheckNodePortWhenTerminating 验证：当所有本地端点都处于 terminating 状态时，
// 不会启用健康检查节点端口。
func TestHealthCheckNodePortWhenTerminating(t *testing.T) {
	_, fp := NewFakeProxier(v1.IPv4Protocol)
	fp.OnServiceSynced()
	fp.OnEndpointSlicesSynced()

	serviceName := "svc1"
	namespaceName := "ns1"

	fp.OnServiceAdd(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespaceName},
		Spec: v1.ServiceSpec{
			ClusterIP: "172.30.1.1",
			Selector:  map[string]string{"foo": "bar"},
			Ports:     []v1.ServicePort{{Name: "", TargetPort: intstr.FromInt32(80), Protocol: v1.ProtocolTCP}},
		},
	})

	endpointSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-1", serviceName),
			Namespace: namespaceName,
			Labels:    map[string]string{discovery.LabelServiceName: serviceName},
		},
		Ports: []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](80),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses:  []string{"10.0.1.1"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To(testNodeName),
		}, {
			Addresses:  []string{"10.0.1.2"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To(testNodeName),
		}, {
			Addresses:  []string{"10.0.1.3"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To(testNodeName),
		}, { // 未就绪端点应被忽略
			Addresses:  []string{"10.0.1.4"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(false)},
			NodeName:   ptr.To(testNodeName),
		}},
	}

	fp.OnEndpointSliceAdd(endpointSlice)
	_ = fp.endpointsMap.Update(fp.endpointsChanges)
	localReadyEndpoints := fp.endpointsMap.LocalReadyEndpoints()
	if len(localReadyEndpoints) != 1 {
		t.Errorf("unexpected number of local ready endpoints, expected 1 but got: %d", len(localReadyEndpoints))
	}

	// 把所有端点都设为 terminating
	endpointSliceTerminating := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-1", serviceName),
			Namespace: namespaceName,
			Labels:    map[string]string{discovery.LabelServiceName: serviceName},
		},
		Ports: []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](80),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses: []string{"10.0.1.1"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(false),
			},
			NodeName: ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.0.1.2"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(true),
			},
			NodeName: ptr.To(testNodeName),
		}, {
			Addresses: []string{"10.0.1.3"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(true),
			},
			NodeName: ptr.To(testNodeName),
		}, { // 未就绪端点应被忽略
			Addresses: []string{"10.0.1.4"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(false),
				Terminating: ptr.To(true),
			},
			NodeName: ptr.To(testNodeName),
		}},
	}

	fp.OnEndpointSliceUpdate(endpointSlice, endpointSliceTerminating)
	_ = fp.endpointsMap.Update(fp.endpointsChanges)
	localReadyEndpoints = fp.endpointsMap.LocalReadyEndpoints()
	if len(localReadyEndpoints) != 0 {
		t.Errorf("unexpected number of local ready endpoints, expected 0 but got: %d", len(localReadyEndpoints))
	}
}

// TODO(thockin): 为 syncProxyRules() 补充更多测试，或进一步拆分函数并分别测试各部分。

// TestInternalTrafficPolicy 校验 internalTrafficPolicy 下 ClusterIP 流量的转发与伪装行为。
func TestInternalTrafficPolicy(t *testing.T) {
	type endpoint struct {
		ip       string
		nodeName string
	}

	testCases := []struct {
		name                  string
		line                  string
		internalTrafficPolicy *v1.ServiceInternalTrafficPolicy
		endpoints             []endpoint
		flowTests             []packetFlowTest
	}{
		{
			name:                  "internalTrafficPolicy is cluster",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyCluster),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to ClusterIP hits all endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.3:80",
					masq:     false,
				},
			},
		},
		{
			name:                  "internalTrafficPolicy is local and there is one local endpoint",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to ClusterIP hits only local endpoint",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80",
					masq:     false,
				},
			},
		},
		{
			name:                  "internalTrafficPolicy is local and there are multiple local endpoints",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", testNodeName},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to ClusterIP hits all local endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80",
					masq:     false,
				},
			},
		},
		{
			name:                  "internalTrafficPolicy is local and there are no local endpoints",
			line:                  getLine(),
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			flowTests: []packetFlowTest{
				{
					name:     "no endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "DROP",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nft, fp := NewFakeProxier(v1.IPv4Protocol)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			serviceName := "svc1"
			namespaceName := "ns1"

			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespaceName},
				Spec: v1.ServiceSpec{
					ClusterIP: "172.30.1.1",
					Selector:  map[string]string{"foo": "bar"},
					Ports:     []v1.ServicePort{{Name: "", Port: 80, Protocol: v1.ProtocolTCP}},
				},
			}
			if tc.internalTrafficPolicy != nil {
				svc.Spec.InternalTrafficPolicy = tc.internalTrafficPolicy
			}

			fp.OnServiceAdd(svc)

			endpointSlice := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", serviceName),
					Namespace: namespaceName,
					Labels:    map[string]string{discovery.LabelServiceName: serviceName},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
			}
			for _, ep := range tc.endpoints {
				endpointSlice.Endpoints = append(endpointSlice.Endpoints, discovery.Endpoint{
					Addresses:  []string{ep.ip},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
					NodeName:   ptr.To(ep.nodeName),
				})
			}

			fp.OnEndpointSliceAdd(endpointSlice)
			fp.syncProxyRules()
			runPacketFlowTests(t, tc.line, nft, testNodeIPs, tc.flowTests)

			fp.OnEndpointSliceDelete(endpointSlice)
			fp.syncProxyRules()
			runPacketFlowTests(t, tc.line, nft, testNodeIPs, []packetFlowTest{
				{
					name:     "endpoints deleted",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
			})
		})
	}
}

// TestTerminatingEndpointsTrafficPolicyLocal 验证：同时存在本地就绪端点与
// “就绪且 terminating”的端点时，只使用就绪端点。
func TestTerminatingEndpointsTrafficPolicyLocal(t *testing.T) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc1", Namespace: "ns1"},
		Spec: v1.ServiceSpec{
			ClusterIP:             "172.30.1.1",
			Type:                  v1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			Ports: []v1.ServicePort{
				{
					Name:       "",
					TargetPort: intstr.FromInt32(80),
					Port:       80,
					Protocol:   v1.ProtocolTCP,
				},
			},
			HealthCheckNodePort: 30000,
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			},
		},
	}

	testcases := []struct {
		name          string
		line          string
		endpointslice *discovery.EndpointSlice
		flowTests     []packetFlowTest
	}{
		{
			name: "ready endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						Addresses: []string{"10.0.1.1"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 存在就绪且非 terminating 的端点，因此外部流量忽略该端点
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 存在就绪且非 terminating 的端点，因此外部流量忽略该端点
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点不在本地，外部流量忽略它
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80",
					masq:     false,
				},
			},
		},
		{
			name: "only terminating endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 只有就绪的 terminating 端点，因此使用该端点
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 只有就绪的 terminating 端点，因此使用该端点
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点既在 terminating 又未就绪，因此不使用
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点不在本地，外部流量忽略它
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.2:80, 10.0.1.3:80",
					masq:     false,
				},
			},
		},
		{
			name: "terminating endpoints on remote node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 该端点非本地，不会被选用，
						// 但它的存在会阻止生成 REJECT 规则
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.5:80",
				},
				{
					name:     "external to LB, no locally-usable endpoints",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "DROP",
				},
			},
		},
		{
			name: "no usable endpoints on any node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 本地端点，但未就绪或不在服务中
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 远端端点，且未就绪或不在服务中
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP, no usable endpoints",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB, no usable endpoints",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			nft, fp := NewFakeProxier(v1.IPv4Protocol)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			fp.OnServiceAdd(service)

			fp.OnEndpointSliceAdd(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, nft, testNodeIPs, testcase.flowTests)

			fp.OnEndpointSliceDelete(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, nft, testNodeIPs, []packetFlowTest{
				{
					name:     "pod to clusterIP after endpoints deleted",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB after endpoints deleted",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			})
		})
	}
}

// TestTerminatingEndpointsTrafficPolicyCluster 验证：同时存在集群范围内就绪端点与
// “就绪且 terminating”的端点时，只使用就绪端点。
func TestTerminatingEndpointsTrafficPolicyCluster(t *testing.T) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc1", Namespace: "ns1"},
		Spec: v1.ServiceSpec{
			ClusterIP:             "172.30.1.1",
			Type:                  v1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyCluster,
			Ports: []v1.ServicePort{
				{
					Name:       "",
					TargetPort: intstr.FromInt32(80),
					Port:       80,
					Protocol:   v1.ProtocolTCP,
				},
			},
			HealthCheckNodePort: 30000,
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			},
		},
	}

	testcases := []struct {
		name          string
		line          string
		endpointslice *discovery.EndpointSlice
		flowTests     []packetFlowTest
	}{
		{
			name: "ready endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						Addresses: []string{"10.0.1.1"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 存在就绪且非 terminating 的端点，因此忽略该端点
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
					{
						// 该端点未在服务中，因此忽略
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
					{
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						NodeName: ptr.To("another-node"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.1:80, 10.0.1.2:80, 10.0.1.5:80",
					masq:     true,
				},
			},
		},
		{
			name: "only terminating endpoints exist",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 只有就绪的 terminating 端点，因此使用该端点
						Addresses: []string{"10.0.1.2"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 只有就绪的 terminating 端点，因此使用该端点
						Addresses: []string{"10.0.1.3"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 该端点既在 terminating 又未就绪，因此不使用
						Addresses: []string{"10.0.1.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
					{
						// 只有就绪的 terminating 端点，因此使用该端点
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("another-node"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.2:80, 10.0.1.3:80, 10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.2:80, 10.0.1.3:80, 10.0.1.5:80",
					masq:     true,
				},
			},
		},
		{
			name: "terminating endpoints on remote node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(true),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "10.0.1.5:80",
					masq:     false,
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "10.0.1.5:80",
					masq:     true,
				},
			},
		},
		{
			name: "no usable endpoints on any node",
			line: getLine(),
			endpointslice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", "svc1"),
					Namespace: "ns1",
					Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{
					{
						// 本地端点，但未就绪或不在服务中
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To(testNodeName),
					},
					{
						// 远端端点，且未就绪或不在服务中
						Addresses: []string{"10.0.1.5"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(false),
							Serving:     ptr.To(false),
							Terminating: ptr.To(true),
						},
						NodeName: ptr.To("node-1"),
					},
				},
			},
			flowTests: []packetFlowTest{
				{
					name:     "pod to clusterIP",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {

			nft, fp := NewFakeProxier(v1.IPv4Protocol)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			fp.OnServiceAdd(service)

			fp.OnEndpointSliceAdd(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, nft, testNodeIPs, testcase.flowTests)

			fp.OnEndpointSliceDelete(testcase.endpointslice)
			fp.syncProxyRules()
			runPacketFlowTests(t, testcase.line, nft, testNodeIPs, []packetFlowTest{
				{
					name:     "pod to clusterIP after endpoints deleted",
					sourceIP: "10.0.0.2",
					destIP:   "172.30.1.1",
					destPort: 80,
					output:   "REJECT",
				},
				{
					name:     "external to LB after endpoints deleted",
					sourceIP: testExternalClient,
					destIP:   "1.2.3.4",
					destPort: 80,
					output:   "REJECT",
				},
			})
		})
	}
}

// TestInternalExternalMasquerade 覆盖 LocalTrafficDetector、externalTrafficPolicy、
// internalTrafficPolicy 和 masqueradeAll 组合下的各类报文流与伪装预期。
func TestInternalExternalMasquerade(t *testing.T) {
	// 把测试初始化代码放进内部函数，这样它能放在对其运行测试用例的前面。
	setupTest := func(fp *Proxier) {
		makeServiceMap(fp,
			makeTestService("ns1", "svc1", func(svc *v1.Service) {
				svc.Spec.Type = "LoadBalancer"
				svc.Spec.ClusterIP = "172.30.0.41"
				svc.Spec.Ports = []v1.ServicePort{{
					Name:     "p80",
					Port:     80,
					Protocol: v1.ProtocolTCP,
					NodePort: int32(3001),
				}}
				svc.Spec.HealthCheckNodePort = 30001
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			}),
			makeTestService("ns2", "svc2", func(svc *v1.Service) {
				svc.Spec.Type = "LoadBalancer"
				svc.Spec.ClusterIP = "172.30.0.42"
				svc.Spec.Ports = []v1.ServicePort{{
					Name:     "p80",
					Port:     80,
					Protocol: v1.ProtocolTCP,
					NodePort: int32(3002),
				}}
				svc.Spec.HealthCheckNodePort = 30002
				svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "5.6.7.8",
				}}
			}),
			makeTestService("ns3", "svc3", func(svc *v1.Service) {
				svc.Spec.Type = "LoadBalancer"
				svc.Spec.ClusterIP = "172.30.0.43"
				svc.Spec.Ports = []v1.ServicePort{{
					Name:     "p80",
					Port:     80,
					Protocol: v1.ProtocolTCP,
					NodePort: int32(3003),
				}}
				svc.Spec.HealthCheckNodePort = 30003
				svc.Spec.InternalTrafficPolicy = ptr.To(v1.ServiceInternalTrafficPolicyLocal)
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "9.10.11.12",
				}}
			}),
		)

		populateEndpointSlices(fp,
			makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{
					{
						Addresses: []string{"10.180.0.1"},
						NodeName:  ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.180.1.1"},
						NodeName:  ptr.To("remote"),
					},
				}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p80"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
			}),
			makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{
					{
						Addresses: []string{"10.180.0.2"},
						NodeName:  ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.180.1.2"},
						NodeName:  ptr.To("remote"),
					},
				}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p80"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
			}),
			makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
				eps.AddressType = discovery.AddressTypeIPv4
				eps.Endpoints = []discovery.Endpoint{
					{
						Addresses: []string{"10.180.0.3"},
						NodeName:  ptr.To(testNodeName),
					},
					{
						Addresses: []string{"10.180.1.3"},
						NodeName:  ptr.To("remote"),
					},
				}
				eps.Ports = []discovery.EndpointPort{{
					Name:     ptr.To("p80"),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
			}),
		)

		fp.syncProxyRules()
	}

	// 所有测试用例共用同一组 flowTests；其中的 "output" 和 "masq" 取值
	// 表示常规情况（本地地址探测可用、未开启 masqueradeAll）。
	flowTests := []packetFlowTest{
		{
			name:     "pod to ClusterIP",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     false,
		},
		{
			name:     "pod to NodePort",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "pod to LB",
			sourceIP: "10.0.0.2",
			destIP:   "1.2.3.4",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "node to ClusterIP",
			sourceIP: testNodeIP,
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "node to NodePort",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "node to LB",
			sourceIP: testNodeIP,
			destIP:   "1.2.3.4",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "external to ClusterIP",
			sourceIP: testExternalClient,
			destIP:   "172.30.0.41",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "external to NodePort",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: 3001,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "external to LB",
			sourceIP: testExternalClient,
			destIP:   "1.2.3.4",
			destPort: 80,
			output:   "10.180.0.1:80, 10.180.1.1:80",
			masq:     true,
		},
		{
			name:     "pod to ClusterIP with eTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.42",
			destPort: 80,

			// externalTrafficPolicy 不适用于 ClusterIP 流量，
			// 因此与 "Pod to ClusterIP" 情况相同
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   false,
		},
		{
			name:     "pod to NodePort with eTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3002,

			// 参见下文 "pod to LB with eTP:Local" 用例的注释。
			// 此处做短路其实并不太合理：从集群外直接访问 NodePort 时，
			// 本来只会得到本地端点。但为了简化实现，也为了用户使用更方便，
			// 这里同样让该情况走短路逻辑。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   false,
		},
		{
			name:     "pod to LB with eTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "5.6.7.8",
			destPort: 80,

			// 短路规则的目的是让行为与报文真正出集群访问 LB、再回到集群时一致。
			// 因此流量会被路由到所有端点，而不只是本地端点。
			// 实际上若报文真的离开集群就必须被伪装；但短路路径可以避免伪装，
			// 而不伪装对用户更有用，所以这里选择不伪装。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   false,
		},
		{
			name:     "node to ClusterIP with eTP:Local",
			sourceIP: testNodeIP,
			destIP:   "172.30.0.42",
			destPort: 80,

			// externalTrafficPolicy 不适用于 ClusterIP 流量，
			// 因此与 "node to ClusterIP" 情况相同
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "node to NodePort with eTP:Local",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 3001,

			// 流量被短路，忽略 externalTrafficPolicy，
			// 因此与上面的 "node to NodePort" 相同。
			output: "10.180.0.1:80, 10.180.1.1:80",
			masq:   true,
		},
		{
			name:     "node to LB with eTP:Local",
			sourceIP: testNodeIP,
			destIP:   "5.6.7.8",
			destPort: 80,

			// 流量被短路，忽略 externalTrafficPolicy，
			// 因此与上面的 "node to LB" 相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "external to ClusterIP with eTP:Local",
			sourceIP: testExternalClient,
			destIP:   "172.30.0.42",
			destPort: 80,

			// externalTrafficPolicy 不适用于 ClusterIP 流量，
			// 因此与上面的 "external to ClusterIP" 相同。
			output: "10.180.0.2:80, 10.180.1.2:80",
			masq:   true,
		},
		{
			name:     "external to NodePort with eTP:Local",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: 3002,

			// externalTrafficPolicy 生效：只选中本地端点，
			// 且不做伪装。
			output: "10.180.0.2:80",
			masq:   false,
		},
		{
			name:     "external to LB with eTP:Local",
			sourceIP: testExternalClient,
			destIP:   "5.6.7.8",
			destPort: 80,

			// externalTrafficPolicy 生效：只选中本地端点，
			// 且不做伪装。
			output: "10.180.0.2:80",
			masq:   false,
		},
		{
			name:     "pod to ClusterIP with iTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "172.30.0.43",
			destPort: 80,

			// internalTrafficPolicy 生效：只选中本地端点。
			output: "10.180.0.3:80",
			masq:   false,
		},
		{
			name:     "pod to NodePort with iTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   testNodeIP,
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与上面的 "pod to NodePort" 相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "pod to LB with iTP:Local",
			sourceIP: "10.0.0.2",
			destIP:   "9.10.11.12",
			destPort: 80,

			// internalTrafficPolicy 不适用于 LoadBalancer 流量，
			// 因此与上面的 "pod to LB" 相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "node to ClusterIP with iTP:Local",
			sourceIP: testNodeIP,
			destIP:   "172.30.0.43",
			destPort: 80,

			// internalTrafficPolicy 生效：只选中本地端点。
			// 与 "node to ClusterIP" 一样会做伪装，
			// 因为 internalTrafficPolicy 不影响伪装行为。
			output: "10.180.0.3:80",
			masq:   true,
		},
		{
			name:     "node to NodePort with iTP:Local",
			sourceIP: testNodeIP,
			destIP:   testNodeIP,
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与上面的 "node to NodePort" 相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "node to LB with iTP:Local",
			sourceIP: testNodeIP,
			destIP:   "9.10.11.12",
			destPort: 80,

			// internalTrafficPolicy 不适用于 LoadBalancer 流量，
			// 因此与上面的 "node to LB" 相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "external to ClusterIP with iTP:Local",
			sourceIP: testExternalClient,
			destIP:   "172.30.0.43",
			destPort: 80,

			// internalTrafficPolicy 生效：只选中本地端点。
			// 与 "external to ClusterIP" 一样会做伪装，
			// 因为 internalTrafficPolicy 不影响伪装行为。
			output: "10.180.0.3:80",
			masq:   true,
		},
		{
			name:     "external to NodePort with iTP:Local",
			sourceIP: testExternalClient,
			destIP:   testNodeIP,
			destPort: 3003,

			// internalTrafficPolicy 不适用于 NodePort 流量，
			// 因此与上面的 "external to NodePort" 相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
		{
			name:     "external to LB with iTP:Local",
			sourceIP: testExternalClient,
			destIP:   "9.10.11.12",
			destPort: 80,

			// internalTrafficPolicy 不适用于 LoadBalancer 流量，
			// 因此与上面的 "external to LB" 相同。
			output: "10.180.0.3:80, 10.180.1.3:80",
			masq:   true,
		},
	}

	type packetFlowTestOverride struct {
		output *string
		masq   *bool
	}

	testCases := []struct {
		name          string
		line          string
		masqueradeAll bool
		localDetector bool
		overrides     map[string]packetFlowTestOverride
	}{
		{
			name:          "base",
			line:          getLine(),
			masqueradeAll: false,
			localDetector: true,
			overrides:     nil,
		},
		{
			name:          "no LocalTrafficDetector",
			line:          getLine(),
			masqueradeAll: false,
			localDetector: false,
			overrides: map[string]packetFlowTestOverride{
				// 没有 LocalTrafficDetector 时，所有访问 ClusterIP 的流量
				// 都被假定来自 Pod，因此不需要伪装。
				"node to ClusterIP": {
					masq: ptr.To(false),
				},
				"node to ClusterIP with eTP:Local": {
					masq: ptr.To(false),
				},
				"node to ClusterIP with iTP:Local": {
					masq: ptr.To(false),
				},
				"external to ClusterIP": {
					masq: ptr.To(false),
				},
				"external to ClusterIP with eTP:Local": {
					masq: ptr.To(false),
				},
				"external to ClusterIP with iTP:Local": {
					masq: ptr.To(false),
				},

				// Pod 流量不存在 eTP:Local 短路逻辑，
				// 因此只能到达本地端点。
				"pod to NodePort with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
				"pod to LB with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
			},
		},
		{
			name:          "masqueradeAll",
			line:          getLine(),
			masqueradeAll: true,
			localDetector: true,
			overrides: map[string]packetFlowTestOverride{
				// 使用 --masquerade-all 时，所有 "to ClusterIP" 流量都会被伪装。
				"pod to ClusterIP": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with eTP:Local": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with iTP:Local": {
					masq: ptr.To(true),
				},
			},
		},
		{
			name:          "masqueradeAll, no LocalTrafficDetector",
			line:          getLine(),
			masqueradeAll: true,
			localDetector: false,
			overrides: map[string]packetFlowTestOverride{
				// 与 "masqueradeAll" 情况相同
				"pod to ClusterIP": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with eTP:Local": {
					masq: ptr.To(true),
				},
				"pod to ClusterIP with iTP:Local": {
					masq: ptr.To(true),
				},

				// 与 "no LocalTrafficDetector" 情况相同
				"pod to NodePort with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
				"pod to LB with eTP:Local": {
					output: ptr.To("10.180.0.2:80"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nft, fp := NewFakeProxier(v1.IPv4Protocol)
			fp.masqueradeAll = tc.masqueradeAll
			if !tc.localDetector {
				fp.localDetector = proxyutil.NewNoOpLocalDetector()
			}
			setupTest(fp)

			// 将基础 flowTests 与各用例的覆盖项合并
			tcFlowTests := make([]packetFlowTest, len(flowTests))
			overridesApplied := 0
			for i := range flowTests {
				tcFlowTests[i] = flowTests[i]
				if overrides, set := tc.overrides[flowTests[i].name]; set {
					overridesApplied++
					if overrides.masq != nil {
						if tcFlowTests[i].masq == *overrides.masq {
							t.Errorf("%q override value for masq is same as base value", flowTests[i].name)
						}
						tcFlowTests[i].masq = *overrides.masq
					}
					if overrides.output != nil {
						if tcFlowTests[i].output == *overrides.output {
							t.Errorf("%q override value for output is same as base value", flowTests[i].name)
						}
						tcFlowTests[i].output = *overrides.output
					}
				}
			}
			if overridesApplied != len(tc.overrides) {
				t.Errorf("%d overrides did not match any test case name!", len(tc.overrides)-overridesApplied)
			}
			runPacketFlowTests(t, tc.line, nft, testNodeIPs, tcFlowTests)
		})
	}
}

// assertNumOperations 断言 nft 上最后一个事务的操作数等于 ops 中各项之和。
// ops 支持传入多个值只是为了便于下面的调用方说明计算过程。
func assertNumOperations(t *testing.T, nft *knftables.Fake, ops ...int) {
	t.Helper()
	expectedOps := 0
	for _, n := range ops {
		expectedOps += n
	}
	if nft.LastTransaction.NumOperations() != expectedOps {
		t.Errorf("Expected transaction with %d operations, got %d", expectedOps, nft.LastTransaction.NumOperations())
	}
}

// 测试多次调用 syncProxyRules()，期间穿插各种变更
// TestSyncProxyRulesRepeated 反复同步并穿插 Service/端点变更，校验增量规则、
// 幂等性及过期链删除的事务行为。
func TestSyncProxyRulesRepeated(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)

	// 辅助函数：从过期链删除逻辑的角度模拟时间已经流逝。
	ageStaleChains := func() {
		for chain, t := range fp.staleChains {
			fp.staleChains[chain] = t.Add(-2 * time.Second)
		}
	}

	// 创建初始状态
	var svc2 *v1.Service

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc2 = svc
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p8080",
				Port:     8080,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.1.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.2.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p8080"),
				Port:     ptr.To[int32](8080),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.42 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.42 . tcp . 8080 : goto service-MHHHYRWA-ns2/svc2/tcp/p8080 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.2.1 . 10.0.2.1 . tcp . 8080 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080
		add rule ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080 meta l4proto tcp dnat to 10.0.2.1:8080
                `)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())

	// 新增一个 Service 及其端点
	makeServiceMap(fp,
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)
	var eps3 *discovery.EndpointSlice
	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns3", "svc3", 1, func(eps *discovery.EndpointSlice) {
			eps3 = eps
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.3.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	fp.syncProxyRules()

	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.42 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.42 . tcp . 8080 : goto service-MHHHYRWA-ns2/svc2/tcp/p8080 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.2.1 . 10.0.2.1 . tcp . 8080 }
		add element ip kube-proxy hairpin-connections { 10.0.3.1 . 10.0.3.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080
		add rule ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080 meta l4proto tcp dnat to 10.0.2.1:8080

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.0.3.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		3, // 分别向 cluster-ips、service-ips 和 hairpin-connections 添加 1 个元素
		3, // 添加并清空服务链，再添加 1 条规则
	)

	// 删除一个 Service；其链会被清空，但不会立即删除。
	fp.OnServiceDelete(svc2)
	fp.syncProxyRules()
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.1 . 10.0.3.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.0.3.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		3, // 分别从 cluster-ips、service-ips 和 hairpin-connections 删除 1 个元素
		1, // 清空服务链
	)

	// 模拟时间流逝，确认过期链被删除。
	ageStaleChains()
	fp.syncProxyRules()
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.1 . 10.0.3.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.0.3.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	// 删除过期链在独立事务中完成，其他内容均未变化
	assertNumOperations(t, nft, 0)

	// 先新增 Service 并同步，再加入其端点。
	makeServiceMap(fp,
		makeTestService("ns4", "svc4", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.44"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)
	fp.syncProxyRules()
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.1 . 10.0.3.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.0.3.1:80

		add element ip kube-proxy no-endpoint-services { 172.30.0.44 . tcp . 80 comment "ns4/svc4:p80" : goto reject-chain }
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		2, // 分别向 cluster-ips 和 no-endpoint-services 添加 1 个元素
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns4", "svc4", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.4.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	fp.syncProxyRules()
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.1 . 10.0.3.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.4.1 . 10.0.4.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.0.3.1:80

		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat to 10.0.4.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		2, // 向 service-ips 添加 1 个元素，从 no-endpoint-services 删除 1 个元素
		1, // 向 hairpin-connections 添加 1 个元素
		3, // 添加并清空服务链，再添加 1 条规则
	)

	// 修改某个已有 Service 的端点。
	eps3update := eps3.DeepCopy()
	eps3update.Endpoints[0].Addresses[0] = "10.0.3.2"
	fp.OnEndpointSliceUpdate(eps3, eps3update)
	fp.syncProxyRules()

	// 旧端点链（对应 10.0.3.1）此时还不会删除。
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.2 . 10.0.3.2 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.4.1 . 10.0.4.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat to 10.0.3.2:80

		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat to 10.0.4.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		2, // 从 hairpin-connections 删除 1 个旧元素并添加 1 个新元素
		3, // 添加并清空服务链，再添加 1 条规则
	)

	// 确保旧的 svc3 链在下次同步时被删除。
	ageStaleChains()

	// 给某 Service 增加一个端点。
	eps3update2 := eps3update.DeepCopy()
	eps3update2.Endpoints = append(eps3update2.Endpoints, discovery.Endpoint{
		Addresses: []string{"10.0.3.3"},
		NodeName:  ptr.To(testNodeName),
	})
	fp.OnEndpointSliceUpdate(eps3update, eps3update2)
	fp.syncProxyRules()

	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.2 . 10.0.3.2 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.3 . 10.0.3.3 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.4.1 . 10.0.4.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat ip addr . port to numgen random mod 2 map { 0 : 10.0.3.2 . 80 , 1 : 10.0.3.3 . 80 }

		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat to 10.0.4.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	// 第一条端点链未变化，但代码仍然会重建它。
	assertNumOperations(t, nft,
		1, // 向 hairpin-connections 添加 1 个元素
		3, // 添加并清空服务链，再添加 1 条规则
	)

	// 清空某 Service 的端点；其链会被清空，但不会立即删除。
	eps3update3 := eps3update2.DeepCopy()
	eps3update3.Endpoints = []discovery.Endpoint{}
	fp.OnEndpointSliceUpdate(eps3update2, eps3update3)
	fp.syncProxyRules()
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy no-endpoint-services { 172.30.0.43 . tcp . 80 comment "ns3/svc3:p80" : goto reject-chain }
		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.4.1 . 10.0.4.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80

		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat to 10.0.4.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		2, // 从 service-ips 删除 1 个元素，向 no-endpoint-services 添加 1 个元素
		2, // 从 hairpin-connections 删除 2 个元素
		1, // 清空服务链
	)

	expectedStaleChains := sets.NewString("service-4AT6LBPK-ns3/svc3/tcp/p80")
	gotStaleChains := sets.StringKeySet(fp.staleChains)
	if !expectedStaleChains.Equal(gotStaleChains) {
		t.Errorf("expected stale chains %v, got %v", expectedStaleChains, gotStaleChains)
	}
	// 立刻把端点恢复为非空；其链会被恢复，并从 staleChains 中移除。
	fp.OnEndpointSliceUpdate(eps3update3, eps3update2)
	fp.syncProxyRules()
	expected = baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy cluster-ips { 172.30.0.44 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.43 . tcp . 80 : goto service-4AT6LBPK-ns3/svc3/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.44 . tcp . 80 : goto service-LAUZTJTB-ns4/svc4/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.2 . 10.0.3.2 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.3.3 . 10.0.3.3 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.4.1 . 10.0.4.1 . tcp . 80 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add chain ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80
		add rule ip kube-proxy service-4AT6LBPK-ns3/svc3/tcp/p80 meta l4proto tcp dnat ip addr . port to numgen random mod 2 map { 0 : 10.0.3.2 . 80 , 1 : 10.0.3.3 . 80 }

		add chain ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80
		add rule ip kube-proxy service-LAUZTJTB-ns4/svc4/tcp/p80 meta l4proto tcp dnat to 10.0.4.1:80
		`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		2, // 从 no-endpoint-services 删除 1 个元素，向 service-ips 添加 1 个元素
		2, // 向 hairpin-connections 添加 2 个元素
		3, // 添加并清空服务链，再添加 1 条规则
	)

	if len(fp.staleChains) != 0 {
		t.Errorf("unexpected stale chains: %v", fp.staleChains)
	}

	// 清空某 Service 的端点，并在过期链老化后再恢复。
	// - 第一次同步时链被清空，但不会立即删除。
	// - 第二次同步时链先被删除，然后重建。
	fp.OnEndpointSliceUpdate(eps3update2, eps3update3)
	fp.syncProxyRules()
	ageStaleChains()
	fp.OnEndpointSliceUpdate(eps3update3, eps3update2)
	fp.syncProxyRules()
	// 第二次变更抵消了第一次，因此预期规则与上次相同
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())

	// 没有新变更的同步，预期规则与上次相同
	fp.syncProxyRules()
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft, 0)
}

// TestSyncProxyRulesStartup 验证从已有 nftables 状态启动时，
// 首次同步只需补齐实际状态与期望状态之间的差异。
func TestSyncProxyRulesStartup(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	fp.syncProxyRules()
	// 统计初始同步所需的操作数
	setupOps := nft.LastTransaction.NumOperations()

	// 新建一个 proxier，从零开始
	nft, fp = NewFakeProxier(v1.IPv4Protocol)

	// 将部分期望状态写入 nftables
	err := nft.ParseDump(baseRules + dedent.Dedent(`
		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add chain ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080
		
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.0.1.1:80

		add rule ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080 meta l4proto tcp dnat to 10.0.2.1:8080

		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.42 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.42 . tcp . 8080 : goto service-MHHHYRWA-ns2/svc2/tcp/p8080 }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.2.1 . 10.0.2.1 . tcp . 8080 }
	`))

	if err != nil {
		t.Errorf("nft.ParseDump failed: %v", err)
	}

	// 创建与已加载 nftables 状态不同的初始状态：
	// - svc1 增加了第二个端点
	// - 新增 svc3
	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns2", "svc2", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p8080",
				Port:     8080,
				Protocol: v1.ProtocolTCP,
			}}
		}),
		makeTestService("ns3", "svc3", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{"10.0.1.1"},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{"10.0.1.2"},
					NodeName:  ptr.To(testNodeName),
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice("ns2", "svc2", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.0.2.1"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p8080"),
				Port:     ptr.To[int32](8080),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := baseRules + dedent.Dedent(`
		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy cluster-ips { 172.30.0.42 }
		add element ip kube-proxy cluster-ips { 172.30.0.43 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 172.30.0.42 . tcp . 8080 : goto service-MHHHYRWA-ns2/svc2/tcp/p8080 }
		add element ip kube-proxy no-endpoint-services { 172.30.0.43 . tcp . 80 comment "ns3/svc3:p80" : goto reject-chain }
		add element ip kube-proxy hairpin-connections { 10.0.1.1 . 10.0.1.1 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.1.2 . 10.0.1.2 . tcp . 80 }
		add element ip kube-proxy hairpin-connections { 10.0.2.1 . 10.0.2.1 . tcp . 8080 }

		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat ip addr . port to numgen random mod 2 map { 0 : 10.0.1.1 . 80 , 1 : 10.0.1.2 . 80 }

		add chain ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080
		add rule ip kube-proxy service-MHHHYRWA-ns2/svc2/tcp/p8080 meta l4proto tcp dnat to 10.0.2.1:8080
	`)
	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
	assertNumOperations(t, nft,
		setupOps, // nft 初始化
		1,        // 向 hairpin-connections 添加 svc1 的新端点
		2,        // 将 svc3 IP 添加到 cluster-ips 和 no-endpoint-services 集合
		6,        // 添加并清空 2 个服务链，各添加 1 条规则
	)
}

// TestNoEndpointsMetric 验证无端点相关指标在 Service 端点变化时的增减与取值。
func TestNoEndpointsMetric(t *testing.T) {
	type endpoint struct {
		ip       string
		nodeName string
	}

	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)
	testCases := []struct {
		name                                                string
		internalTrafficPolicy                               *v1.ServiceInternalTrafficPolicy
		externalTrafficPolicy                               v1.ServiceExternalTrafficPolicy
		endpoints                                           []endpoint
		expectedSyncProxyRulesNoLocalEndpointsTotalInternal int
		expectedSyncProxyRulesNoLocalEndpointsTotalExternal int
	}{
		{
			name:                  "internalTrafficPolicy is set and there are local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
		},
		{
			name:                  "externalTrafficPolicy is set and there are local endpoints",
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
		},
		{
			name:                  "both policies are set and there are local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", testNodeName},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
		},
		{
			name:                  "internalTrafficPolicy is set and there are no local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			expectedSyncProxyRulesNoLocalEndpointsTotalInternal: 1,
		},
		{
			name:                  "externalTrafficPolicy is set and there are no local endpoints",
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			expectedSyncProxyRulesNoLocalEndpointsTotalExternal: 1,
		},
		{
			name:                  "both policies are set and there are no local endpoints",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints: []endpoint{
				{"10.0.1.1", "node0"},
				{"10.0.1.2", "node1"},
				{"10.0.1.3", "node2"},
			},
			expectedSyncProxyRulesNoLocalEndpointsTotalInternal: 1,
			expectedSyncProxyRulesNoLocalEndpointsTotalExternal: 1,
		},
		{
			name:                  "both policies are set and there are no endpoints at all",
			internalTrafficPolicy: ptr.To(v1.ServiceInternalTrafficPolicyLocal),
			externalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			endpoints:             []endpoint{},
			expectedSyncProxyRulesNoLocalEndpointsTotalInternal: 0,
			expectedSyncProxyRulesNoLocalEndpointsTotalExternal: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, fp := NewFakeProxier(v1.IPv4Protocol)
			fp.OnServiceSynced()
			fp.OnEndpointSlicesSynced()

			serviceName := "svc1"
			namespaceName := "ns1"

			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespaceName},
				Spec: v1.ServiceSpec{
					ClusterIP: "172.30.1.1",
					Selector:  map[string]string{"foo": "bar"},
					Ports:     []v1.ServicePort{{Name: "", Port: 80, Protocol: v1.ProtocolTCP, NodePort: 123}},
				},
			}
			if tc.internalTrafficPolicy != nil {
				svc.Spec.InternalTrafficPolicy = tc.internalTrafficPolicy
			}
			if tc.externalTrafficPolicy != "" {
				svc.Spec.Type = v1.ServiceTypeNodePort
				svc.Spec.ExternalTrafficPolicy = tc.externalTrafficPolicy
			}

			fp.OnServiceAdd(svc)

			endpointSlice := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-1", serviceName),
					Namespace: namespaceName,
					Labels:    map[string]string{discovery.LabelServiceName: serviceName},
				},
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Port:     ptr.To[int32](80),
					Protocol: ptr.To(v1.ProtocolTCP),
				}},
				AddressType: discovery.AddressTypeIPv4,
			}
			for _, ep := range tc.endpoints {
				endpointSlice.Endpoints = append(endpointSlice.Endpoints, discovery.Endpoint{
					Addresses:  []string{ep.ip},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
					NodeName:   ptr.To(ep.nodeName),
				})
			}

			fp.OnEndpointSliceAdd(endpointSlice)
			fp.syncProxyRules()
			syncProxyRulesNoLocalEndpointsTotalInternal, err := testutil.GetGaugeMetricValue(metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("internal", string(fp.ipFamily)))
			if err != nil {
				t.Errorf("failed to get %s value, err: %v", metrics.SyncProxyRulesNoLocalEndpointsTotal.Name, err)
			}

			if tc.expectedSyncProxyRulesNoLocalEndpointsTotalInternal != int(syncProxyRulesNoLocalEndpointsTotalInternal) {
				t.Errorf("sync_proxy_rules_no_endpoints_total metric mismatch(internal): got=%d, expected %d", int(syncProxyRulesNoLocalEndpointsTotalInternal), tc.expectedSyncProxyRulesNoLocalEndpointsTotalInternal)
			}

			syncProxyRulesNoLocalEndpointsTotalExternal, err := testutil.GetGaugeMetricValue(metrics.SyncProxyRulesNoLocalEndpointsTotal.WithLabelValues("external", string(fp.ipFamily)))
			if err != nil {
				t.Errorf("failed to get %s value(external), err: %v", metrics.SyncProxyRulesNoLocalEndpointsTotal.Name, err)
			}

			if tc.expectedSyncProxyRulesNoLocalEndpointsTotalExternal != int(syncProxyRulesNoLocalEndpointsTotalExternal) {
				t.Errorf("sync_proxy_rules_no_endpoints_total metric mismatch(internal): got=%d, expected %d", int(syncProxyRulesNoLocalEndpointsTotalExternal), tc.expectedSyncProxyRulesNoLocalEndpointsTotalExternal)
			}
		})
	}
}

// TestLoadBalancerIngressRouteTypeProxy 验证 LoadBalancerIngress 的端口协议与
// Proxy 路由类型对 LB 规则生成的影响。
func TestLoadBalancerIngressRouteTypeProxy(t *testing.T) {
	testCases := []struct {
		name         string
		svcIP        string
		svcLBIP      string
		ipMode       *v1.LoadBalancerIPMode
		expectedRule bool
	}{
		{
			name:         "ipMode Proxy",
			svcIP:        "10.20.30.41",
			svcLBIP:      "1.2.3.4",
			ipMode:       ptr.To(v1.LoadBalancerIPModeProxy),
			expectedRule: false,
		},
		{
			name:         "ipMode VIP",
			svcIP:        "10.20.30.42",
			svcLBIP:      "1.2.3.5",
			ipMode:       ptr.To(v1.LoadBalancerIPModeVIP),
			expectedRule: true,
		},
		{
			name:         "ipMode nil",
			svcIP:        "10.20.30.43",
			svcLBIP:      "1.2.3.6",
			ipMode:       nil,
			expectedRule: true,
		},
	}

	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			nft, fp := NewFakeProxier(v1.IPv4Protocol)
			makeServiceMap(fp,
				makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
					svc.Spec.Type = "LoadBalancer"
					svc.Spec.ClusterIP = testCase.svcIP
					svc.Spec.Ports = []v1.ServicePort{{
						Name:     svcPortName.Port,
						Port:     int32(svcPort),
						Protocol: v1.ProtocolTCP,
						NodePort: int32(svcNodePort),
					}}
					svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
						IP:     testCase.svcLBIP,
						IPMode: testCase.ipMode,
					}}
				}),
			)

			populateEndpointSlices(fp,
				makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
					eps.AddressType = discovery.AddressTypeIPv4
					eps.Endpoints = []discovery.Endpoint{{
						Addresses: []string{"10.180.0.1"},
						NodeName:  ptr.To(testNodeName),
					}}
					eps.Ports = []discovery.EndpointPort{{
						Name:     ptr.To("p80"),
						Port:     ptr.To[int32](80),
						Protocol: ptr.To(v1.ProtocolTCP),
					}}
				}),
			)

			fp.syncProxyRules()

			element := nft.Table.Maps["service-ips"].FindElement(testCase.svcLBIP, "tcp", "80")
			ruleExists := element != nil
			if ruleExists != testCase.expectedRule {
				t.Errorf("unexpected rule for %s", testCase.svcLBIP)
			}
		})
	}
}

// Test_servicePortChainNameBase 校验服务链名基名的生成，包括超长名称的哈希截断。
func Test_servicePortChainNameBase(t *testing.T) {
	testCases := []struct {
		name     string
		spn      proxy.ServicePortName
		protocol string
		expected string
	}{
		{
			name: "simple",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "testing",
					Name:      "service",
				},
				Port: "http",
			},
			protocol: "tcp",
			expected: "P4ZYZVCF-testing/service/tcp/http",
		},
		{
			name: "different port, different hash",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "testing",
					Name:      "service",
				},
				Port: "https",
			},
			protocol: "tcp",
			expected: "LZBRENCP-testing/service/tcp/https",
		},
		{
			name: "max length",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx",
					Name:      "very-long-service-name-why-would-you-even-do-this-i-mean-really",
				},
				Port: "port-443-providing-the-hypertext-transmission-protocol-with-tls",
			},
			protocol: "sctp",
			expected: "KR6NACJP-very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx/very-long-service-name-why-would-you-even-do-this-i-mean-really/sctp/port-443-providing-the-hypertext-transmission-protocol-with-tls",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			name := servicePortChainNameBase(&tc.spn, tc.protocol)
			if name != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, name)
			}
		})
	}
}

// Test_servicePortEndpointChainNameBase 校验端点链名基名的生成，包括端点地址的编码。
func Test_servicePortEndpointChainNameBase(t *testing.T) {
	testCases := []struct {
		name     string
		spn      proxy.ServicePortName
		protocol string
		endpoint string
		expected string
	}{
		{
			name: "simple",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "testing",
					Name:      "service",
				},
				Port: "http",
			},
			protocol: "tcp",
			endpoint: "10.180.0.1:80",
			expected: "JO2XBXZR-testing/service/tcp/http__10.180.0.1/80",
		},
		{
			name: "different endpoint, different hash",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "testing",
					Name:      "service",
				},
				Port: "http",
			},
			protocol: "tcp",
			endpoint: "10.180.0.2:80",
			expected: "5S6H3H22-testing/service/tcp/http__10.180.0.2/80",
		},
		{
			name: "ipv6",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "testing",
					Name:      "service",
				},
				Port: "http",
			},
			protocol: "tcp",
			endpoint: "[fd80:abcd:12::a1b2:c3d4:e5f6:9999]:80",
			expected: "U7E2ET36-testing/service/tcp/http__fd80.abcd.12..a1b2.c3d4.e5f6.9999/80",
		},
		{
			name: "max length without truncation",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx",
					Name:      "very-long-service-name-why-would-you-even-do-this-i-mean-really",
				},
				Port: "port-443-providing-the-hypertext-transmission-protocol-with-tls",
			},
			protocol: "sctp",
			endpoint: "[1234:5678:9abc:def0::abc:1234]:443",
			expected: "5YS7AFEA-very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx/very-long-service-name-why-would-you-even-do-this-i-mean-really/sctp/port-443-providing-the-hypertext-transmission-protocol-with-tls__1234.5678.9abc.def0..abc.1234/443",
		},
		{
			name: "truncated, 1",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx",
					Name:      "very-long-service-name-why-would-you-even-do-this-i-mean-really",
				},
				Port: "port-443-providing-the-hypertext-transmission-protocol-with-tls",
			},
			protocol: "sctp",
			endpoint: "[1234:5678:9abc:def0::abcd:1234:5678]:443",
			expected: "CI6C53Q3-very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx/very-long-service-name-why-would-you-even-do-this-i-mean-really/sctp/port-443-providing-the-hypertext-transmission-protocol-with-tls__1234.5678.9abc.def0..abcd.1234...",
		},
		{
			name: "truncated, 2 (different IP, which is not visible in the result)",
			spn: proxy.ServicePortName{
				NamespacedName: types.NamespacedName{
					Namespace: "very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx",
					Name:      "very-long-service-name-why-would-you-even-do-this-i-mean-really",
				},
				Port: "port-443-providing-the-hypertext-transmission-protocol-with-tls",
			},
			protocol: "sctp",
			endpoint: "[1234:5678:9abc:def0::abcd:1234:8765]:443",
			expected: "2FLXFK6X-very-long-namespace-name-abcdefghijklmnopqrstuvwxyz0123456789xx/very-long-service-name-why-would-you-even-do-this-i-mean-really/sctp/port-443-providing-the-hypertext-transmission-protocol-with-tls__1234.5678.9abc.def0..abcd.1234...",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			name := servicePortEndpointChainNameBase(&tc.spn, tc.protocol, tc.endpoint)
			if name != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, name)
			}
		})
	}
}

// TestProxier_OnServiceCIDRsChanged 验证 Service CIDR 变化时，
// 丢弃未分配 ClusterIP 的规则随之更新。
func TestProxier_OnServiceCIDRsChanged(t *testing.T) {
	var proxier *Proxier

	proxier = &Proxier{ipFamily: v1.IPv4Protocol}
	proxier.OnServiceCIDRsChanged([]string{"172.30.0.0/16", "fd00:10:96::/112"})
	assert.Equal(t, "172.30.0.0/16", proxier.serviceCIDRs)

	proxier.OnServiceCIDRsChanged([]string{"172.30.0.0/16", "172.50.0.0/16", "fd00:10:96::/112", "fd00:172:30::/112"})
	assert.Equal(t, "172.30.0.0/16,172.50.0.0/16", proxier.serviceCIDRs)

	proxier = &Proxier{ipFamily: v1.IPv6Protocol}
	proxier.OnServiceCIDRsChanged([]string{"172.30.0.0/16", "fd00:10:96::/112"})
	assert.Equal(t, "fd00:10:96::/112", proxier.serviceCIDRs)

	proxier.OnServiceCIDRsChanged([]string{"172.30.0.0/16", "172.50.0.0/16", "fd00:10:96::/112", "fd00:172:30::/112"})
	assert.Equal(t, "fd00:10:96::/112,fd00:172:30::/112", proxier.serviceCIDRs)
}

// TestBadIPs 验证 Service 和 Endpoints 中带前导零的非法 IP/CIDR 会先规范化，避免将无效值传给 nft。
func TestBadIPs(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)

	makeServiceMap(fp,
		makeTestService("ns1", "svc1", func(svc *v1.Service) {
			svc.Spec.Type = "LoadBalancer"
			svc.Spec.ClusterIP = "172.30.0.041"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 3001,
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: "1.2.3.004",
			}}
			svc.Spec.ExternalIPs = []string{"192.168.099.022"}
			svc.Spec.LoadBalancerSourceRanges = []string{"203.0.113.000/025"}
		}),
	)
	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc1", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.00.001"},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	fp.syncProxyRules()

	expected := baseRules + dedent.Dedent(`
		# svc1
		add chain ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy service-ULMVA6XW-ns1/svc1/tcp/p80 meta l4proto tcp dnat to 10.180.0.1:80

		add chain ip kube-proxy external-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy external-ULMVA6XW-ns1/svc1/tcp/p80 mark set mark or 0x4000 comment "masquerade"
		add rule ip kube-proxy external-ULMVA6XW-ns1/svc1/tcp/p80 goto service-ULMVA6XW-ns1/svc1/tcp/p80


		add chain ip kube-proxy firewall-ULMVA6XW-ns1/svc1/tcp/p80
		add rule ip kube-proxy firewall-ULMVA6XW-ns1/svc1/tcp/p80 ip saddr != { 203.0.113.0/25 } drop

		add element ip kube-proxy cluster-ips { 172.30.0.41 }
		add element ip kube-proxy service-ips { 172.30.0.41 . tcp . 80 : goto service-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 192.168.99.22 . tcp . 80 : goto external-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-ips { 1.2.3.4 . tcp . 80 : goto external-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy service-nodeports { tcp . 3001 : goto external-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy firewall-ips { 1.2.3.4 . tcp . 80 : goto firewall-ULMVA6XW-ns1/svc1/tcp/p80 }
		add element ip kube-proxy hairpin-connections { 10.180.0.1 . 10.180.0.1 . tcp . 80 }
		`)

	assertNFTablesTransactionEqual(t, getLine(), expected, nft.Dump())
}

// Test_useLocalhostNodePortProxy 验证 localhost NodePort 用户态代理启用条件的判定：
// 特性开关与回环地址是否列在 nodePortAddresses 中共同决定结果。
func Test_useLocalhostNodePortProxy(t *testing.T) {
	testCases := []struct {
		name              string
		family            v1.IPFamily
		nodePortAddresses []string
		expected          bool
	}{
		{
			name:              "explicit IPv4 loopback activates",
			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"127.0.0.0/8"},
			expected:          true,
		},
		{
			name:              "explicit IPv6 loopback activates",
			family:            v1.IPv6Protocol,
			nodePortAddresses: []string{"::1/128"},
			expected:          true,
		},
		{
			name:              "IPv6 loopback does not activate an IPv4 proxier",
			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"::1/128"},
			expected:          false,
		},
		{
			name:              "IPv4 loopback does not activate an IPv6 proxier",
			family:            v1.IPv6Protocol,
			nodePortAddresses: []string{"127.0.0.0/8"},
			expected:          false,
		},
		{
			name:              "IPv4 all/zero-CIDR does not activate",
			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"0.0.0.0/0"},
			expected:          false,
		},
		{
			name:              "IPv6 all/zero-CIDR does not activate",
			family:            v1.IPv6Protocol,
			nodePortAddresses: []string{"::/0"},
			expected:          false,
		},
		{
			name:              "explicit loopback before all activates",
			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"127.0.0.0/8", "0.0.0.0/0"},
			expected:          true,
		},
		{
			name:              "all before explicit loopback still activates",
			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"0.0.0.0/0", "127.0.0.0/8"},
			expected:          true,
		},
		{
			name:              "non-loopback CIDR does not activate",
			family:            v1.IPv4Protocol,
			nodePortAddresses: []string{"192.168.1.1/32"},
			expected:          false,
		},
		{
			name:              "empty addresses default to match-all and do not activate",
			family:            v1.IPv4Protocol,
			nodePortAddresses: nil,
			expected:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			npa := proxyutil.NewNodePortAddresses(tc.family, tc.nodePortAddresses)
			if got := useLocalhostNodePortProxy(npa); got != tc.expected {
				t.Errorf("useLocalhostNodePortProxy(%v) = %v, want %v", tc.nodePortAddresses, got, tc.expected)
			}
		})
	}
}

// TestLocalhostNodePortReject 验证特性开启但回环未被列入 nodePortAddresses 时，
// localhost NodePort 的 TCP/UDP/SCTP 流量全部被拒绝并计数。
func TestLocalhostNodePortReject(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)

	// 为该 proxier 启用 localhost NodePort 用户态代理。
	fp.nodePortAddresses = proxyutil.NewNodePortAddresses(v1.IPv4Protocol, []string{"127.0.0.0/8"})
	fp.localhostNodePortsEnabled = true
	fp.localhostNodePortProxy = localnodeportproxy.NewLocalNodePortProxy(context.Background(), v1.IPv4Protocol)
	defer fp.localhostNodePortProxy.Shutdown()

	makeServiceMap(fp,
		makeTestService("ns1", "svc-udp", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p53",
				Protocol: v1.ProtocolUDP,
				Port:     53,
				NodePort: 30053,
			}}
		}),
	)
	if err := fp.syncProxyRules(); err != nil {
		t.Fatalf("syncProxyRules failed: %v", err)
	}

	dump := nft.Dump()
	for _, want := range []string{
		"add counter ip kube-proxy localhost-nodeport-rejected-udp",
		"add counter ip kube-proxy localhost-nodeport-rejected-sctp",
		`add rule ip kube-proxy localhost-nodeport-reject-udp counter name "localhost-nodeport-rejected-udp" reject`,
		`add rule ip kube-proxy localhost-nodeport-reject-sctp counter name "localhost-nodeport-rejected-sctp" reject`,
		"add rule ip kube-proxy nodeport-endpoints-check ip daddr 127.0.0.0/8 meta l4proto . th dport vmap @localhost-nodeport-reject-ports",
		"add element ip kube-proxy localhost-nodeport-reject-ports { udp . 30053 : goto localhost-nodeport-reject-udp }",
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("expected dump to contain:\n  %s\nfull dump:\n%s", want, dump)
		}
	}
	// 代理会处理 TCP，因此该协议不会被拒绝。
	if strings.Contains(dump, "localhost-nodeport-rejected-tcp") {
		t.Errorf("did not expect dump to reject TCP:\n%s", dump)
	}
}

// TestLocalhostNodePortRejectAll 验证特性关闭时，localhost NodePort 的所有协议流量
// 均按常规路径处理，不产生拒绝规则。
func TestLocalhostNodePortRejectAll(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)

	// 特性已启用，但 nodePortAddresses 中不含回环地址，
	// 因此 kube-proxy 拒绝所有 localhost NodePort 流量，而不是代理其中任何流量。
	fp.localhostNodePortsEnabled = true

	makeServiceMap(fp,
		makeTestService("ns1", "svc-tcp", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Protocol: v1.ProtocolTCP,
				Port:     80,
				NodePort: 30080,
			}}
		}),
		makeTestService("ns2", "svc-udp", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.0.42"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p53",
				Protocol: v1.ProtocolUDP,
				Port:     53,
				NodePort: 30053,
			}}
		}),
		makeTestService("ns3", "svc-sctp", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.0.43"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p9",
				Protocol: v1.ProtocolSCTP,
				Port:     9,
				NodePort: 30009,
			}}
		}),
	)
	if err := fp.syncProxyRules(); err != nil {
		t.Fatalf("syncProxyRules failed: %v", err)
	}

	dump := nft.Dump()
	for _, want := range []string{
		"add counter ip kube-proxy localhost-nodeport-rejected-tcp",
		"add counter ip kube-proxy localhost-nodeport-rejected-udp",
		"add counter ip kube-proxy localhost-nodeport-rejected-sctp",
		`add rule ip kube-proxy localhost-nodeport-reject-tcp counter name "localhost-nodeport-rejected-tcp" reject`,
		`add rule ip kube-proxy localhost-nodeport-reject-udp counter name "localhost-nodeport-rejected-udp" reject`,
		`add rule ip kube-proxy localhost-nodeport-reject-sctp counter name "localhost-nodeport-rejected-sctp" reject`,
		"add rule ip kube-proxy nodeport-endpoints-check ip daddr 127.0.0.0/8 meta l4proto . th dport vmap @localhost-nodeport-reject-ports",
		"add element ip kube-proxy localhost-nodeport-reject-ports { tcp . 30080 : goto localhost-nodeport-reject-tcp }",
		"add element ip kube-proxy localhost-nodeport-reject-ports { udp . 30053 : goto localhost-nodeport-reject-udp }",
		"add element ip kube-proxy localhost-nodeport-reject-ports { sctp . 30009 : goto localhost-nodeport-reject-sctp }",
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("expected dump to contain:\n  %s\nfull dump:\n%s", want, dump)
		}
	}
}

// TestLocalhostNodePortProxy 验证回环地址在 nodePortAddresses 中时，
// 用户态代理接管 TCP 的 localhost NodePort，UDP/SCTP 仍被拒绝并计数。
func TestLocalhostNodePortProxy(t *testing.T) {
	nft, fp := NewFakeProxier(v1.IPv4Protocol)
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeNFTables)

	// 启用 localhost NodePort 代理：回环地址在 nodePortAddresses 中，
	// 因此用户态代理处理 TCP 的 localhost NodePort，而 UDP/SCTP 仍被拒绝。
	fp.nodePortAddresses = proxyutil.NewNodePortAddresses(v1.IPv4Protocol, []string{fmt.Sprintf("%s/32", testNodeIP), "127.0.0.0/8"})
	fp.localhostNodePortsEnabled = true
	fp.localhostNodePortProxy = localnodeportproxy.NewLocalNodePortProxy(context.Background(), v1.IPv4Protocol)
	defer fp.localhostNodePortProxy.Shutdown()

	makeServiceMap(fp,
		makeTestService("ns1", "svc-tcp", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.30.0.41"
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     "p80",
				Protocol: v1.ProtocolTCP,
				Port:     80,
				NodePort: 30080,
			}}
		}),
	)
	populateEndpointSlices(fp,
		makeTestEndpointSlice("ns1", "svc-tcp", 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{"10.180.0.1"},
				NodeName:  new(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     new("p80"),
				Port:     ptr.To[int32](80),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	if err := fp.syncProxyRules(); err != nil {
		t.Fatalf("syncProxyRules failed: %v", err)
	}

	dump := nft.Dump()
	// 代理会处理 TCP，因此拒绝规则只覆盖它无法代理的协议，
	// TCP NodePort 则按正常路径分发。
	for _, want := range []string{
		"add counter ip kube-proxy localhost-nodeport-rejected-udp",
		"add counter ip kube-proxy localhost-nodeport-rejected-sctp",
		`add rule ip kube-proxy localhost-nodeport-reject-udp counter name "localhost-nodeport-rejected-udp" reject`,
		`add rule ip kube-proxy localhost-nodeport-reject-sctp counter name "localhost-nodeport-rejected-sctp" reject`,
		"add rule ip kube-proxy nodeport-endpoints-check ip daddr 127.0.0.0/8 meta l4proto . th dport vmap @localhost-nodeport-reject-ports",
		"add element ip kube-proxy service-nodeports { tcp . 30080 : goto external-",
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("expected dump to contain:\n  %s\nfull dump:\n%s", want, dump)
		}
	}
	// TCP 由代理处理，因此既不会被拒绝，也不会计入拒绝计数器。
	if strings.Contains(dump, "localhost-nodeport-rejected-tcp") {
		t.Errorf("did not expect TCP to be rejected:\n%s", dump)
	}
	if strings.Contains(dump, "localhost-nodeport-reject-ports { tcp .") {
		t.Errorf("did not expect a TCP localhost reject element:\n%s", dump)
	}
}
