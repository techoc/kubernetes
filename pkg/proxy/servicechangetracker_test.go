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
	"net"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/dump"
	netutils "k8s.io/utils/net"
)

// testHostname 是构造 EndpointsChangeTracker 时使用的节点主机名。
const testHostname = "test-hostname"

// makeTestServiceInfo 构造测试期望的 BaseServicePortInfo，可传入函数继续设置字段。
func makeTestServiceInfo(clusterIP string, port int, protocol string, healthcheckNodePort int, svcInfoFuncs ...func(*BaseServicePortInfo)) *BaseServicePortInfo {
	bsvcPortInfo := &BaseServicePortInfo{
		clusterIP: netutils.ParseIPSloppy(clusterIP),
		port:      port,
		protocol:  v1.Protocol(protocol),
	}
	if healthcheckNodePort != 0 {
		bsvcPortInfo.healthCheckNodePort = healthcheckNodePort
	}
	for _, svcInfoFunc := range svcInfoFuncs {
		svcInfoFunc(bsvcPortInfo)
	}
	return bsvcPortInfo
}

// makeTestService 构造一个最小可用的 Service，并用 svcFunc 填充用例特有字段。
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

// addTestPort 向端口列表追加一个测试用 ServicePort。
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

// makeNSN 由命名空间和名称构造 NamespacedName。
func makeNSN(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// makeServicePortName 构造 ServicePortName，作为 service map 的键。
func makeServicePortName(ns, name, port string, protocol v1.Protocol) ServicePortName {
	return ServicePortName{
		NamespacedName: makeNSN(ns, name),
		Port:           port,
		Protocol:       protocol,
	}
}

// makeIPs 将字符串形式的 IP 列表解析为 net.IP 切片。
func makeIPs(ipStr ...string) []net.IP {
	var ips []net.IP
	for _, s := range ipStr {
		ips = append(ips, netutils.ParseIPSloppy(s))
	}
	return ips
}

// mustMakeCIDRs 解析 CIDR 字符串；解析失败会让测试立即崩溃以便定位。
func mustMakeCIDRs(cidrStr ...string) []*net.IPNet {
	var cidrs []*net.IPNet
	for _, s := range cidrStr {
		if _, n, err := netutils.ParseCIDRSloppy(s); err == nil {
			cidrs = append(cidrs, n)
		} else {
			panic(err)
		}
	}
	return cidrs
}

// TestServiceToServiceMap 验证 Service 到 service map 的转换，重点覆盖 IP 族过滤。
func TestServiceToServiceMap(t *testing.T) {
	testClusterIPv4 := "10.0.0.1"
	testExternalIPv4 := "8.8.8.8"
	testSourceRangeIPv4 := "0.0.0.0/1"
	testClusterIPv6 := "2001:db8:85a3:0:0:8a2e:370:7334"
	testExternalIPv6 := "2001:db8:85a3:0:0:8a2e:370:7335"
	testSourceRangeIPv6 := "2001:db8::/32"
	ipModeVIP := v1.LoadBalancerIPModeVIP
	ipModeProxy := v1.LoadBalancerIPModeProxy

	testCases := []struct {
		desc     string
		service  *v1.Service
		expected map[ServicePortName]*BaseServicePortInfo
		ipFamily v1.IPFamily
	}{
		// 空输入不应产生任何映射。
		{
			desc:     "nothing",
			ipFamily: v1.IPv4Protocol,

			service:  nil,
			expected: map[ServicePortName]*BaseServicePortInfo{},
		},
		// Headless Service 应被忽略。
		{
			desc:     "headless service",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns2", "headless", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeClusterIP
				svc.Spec.ClusterIP = v1.ClusterIPNone
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "rpc", "UDP", 1234, 0, 0)
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{},
		},
		// SCTP 协议的 Headless Service 同样被忽略。
		{
			desc:     "headless sctp service",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns2", "headless", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeClusterIP
				svc.Spec.ClusterIP = v1.ClusterIPNone
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "sip", "SCTP", 7777, 0, 0)
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{},
		},
		{
			desc:     "headless service without port",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns2", "headless-without-port", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeClusterIP
				svc.Spec.ClusterIP = v1.ClusterIPNone
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{},
		},
		// 普通 ClusterIP Service 的每个端口都生成一条映射。
		{
			desc:     "cluster ip service",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns2", "cluster-ip", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeClusterIP
				svc.Spec.ClusterIP = "172.16.55.4"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p1", "UDP", 1234, 4321, 0)
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p2", "UDP", 1235, 5321, 0)
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("ns2", "cluster-ip", "p1", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.4", 1234, "UDP", 0),
				makeServicePortName("ns2", "cluster-ip", "p2", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.4", 1235, "UDP", 0),
			},
		},
		// NodePort Service 的映射使用 ClusterIP 与端口信息。
		{
			desc:     "nodeport service",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns2", "node-port", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeNodePort
				svc.Spec.ClusterIP = "172.16.55.10"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port1", "UDP", 345, 678, 0)
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port2", "TCP", 344, 677, 0)
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("ns2", "node-port", "port1", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.10", 345, "UDP", 0),
				makeServicePortName("ns2", "node-port", "port2", v1.ProtocolTCP): makeTestServiceInfo("172.16.55.10", 344, "TCP", 0),
			},
		},
		// LoadBalancer Ingress IP 会出现在 loadBalancerVIPs 中。
		{
			desc:     "load balancer service",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns1", "load-balancer", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ClusterIP = "172.16.55.11"
				svc.Spec.LoadBalancerIP = "5.6.7.8"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port3", "UDP", 8675, 30061, 7000)
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port4", "UDP", 8676, 30062, 7001)
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: "10.1.2.4"}}
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("ns1", "load-balancer", "port3", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.11", 8675, "UDP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerVIPs = makeIPs("10.1.2.4")
				}),
				makeServicePortName("ns1", "load-balancer", "port4", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.11", 8676, "UDP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerVIPs = makeIPs("10.1.2.4")
				}),
			},
		},
		// ipMode=VIP 时保留 LoadBalancer VIP。
		{
			desc:     "load balancer service ipMode VIP",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns1", "load-balancer", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ClusterIP = "172.16.55.11"
				svc.Spec.LoadBalancerIP = "5.6.7.8"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port3", "UDP", 8675, 30061, 7000)
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port4", "UDP", 8676, 30062, 7001)
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: "10.1.2.4", IPMode: &ipModeVIP}}
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("ns1", "load-balancer", "port3", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.11", 8675, "UDP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerVIPs = makeIPs("10.1.2.4")
				}),
				makeServicePortName("ns1", "load-balancer", "port4", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.11", 8676, "UDP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerVIPs = makeIPs("10.1.2.4")
				}),
			},
		},
		// ipMode=Proxy 时不设置 LoadBalancer VIP，避免绕过 kube-proxy。
		{
			desc:     "load balancer service ipMode Proxy",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns1", "load-balancer", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ClusterIP = "172.16.55.11"
				svc.Spec.LoadBalancerIP = "5.6.7.8"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port3", "UDP", 8675, 30061, 7000)
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port4", "UDP", 8676, 30062, 7001)
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: "10.1.2.4", IPMode: &ipModeProxy}}
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("ns1", "load-balancer", "port3", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.11", 8675, "UDP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
				}),
				makeServicePortName("ns1", "load-balancer", "port4", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.11", 8676, "UDP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
				}),
			},
		},
		// 仅本地流量策略的 LoadBalancer Service 需要记录健康检查端口。
		{
			desc:     "load balancer service with only local traffic policy",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns1", "only-local-load-balancer", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ClusterIP = "172.16.55.12"
				svc.Spec.LoadBalancerIP = "5.6.7.8"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "portx", "UDP", 8677, 30063, 7002)
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "porty", "UDP", 8678, 30064, 7003)
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: "10.1.2.3"}}
				svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
				svc.Spec.HealthCheckNodePort = 345
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("ns1", "only-local-load-balancer", "portx", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.12", 8677, "UDP", 345, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerVIPs = makeIPs("10.1.2.3")
				}),
				makeServicePortName("ns1", "only-local-load-balancer", "porty", v1.ProtocolUDP): makeTestServiceInfo("172.16.55.12", 8678, "UDP", 345, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerVIPs = makeIPs("10.1.2.3")
				}),
			},
		},
		// ExternalName Service 即使带有 ClusterIP 和端口，也不应被代理。
		{
			desc:     "external name service",
			ipFamily: v1.IPv4Protocol,

			service: makeTestService("ns2", "external-name", func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeExternalName
				svc.Spec.ClusterIP = "172.16.55.4" // 应被忽略：ExternalName 类型不使用 ClusterIP
				svc.Spec.ExternalName = "foo2.bar.com"
				svc.Spec.Ports = addTestPort(svc.Spec.Ports, "portz", "UDP", 1235, 5321, 0)
			}),
			expected: map[ServicePortName]*BaseServicePortInfo{},
		},
		// IPv4 模式下应过滤掉 IPv6 的 ClusterIP。
		{
			desc:     "service with ipv6 clusterIP under ipv4 mode, service should be filtered",
			ipFamily: v1.IPv4Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalidIPv6InIPV4Mode",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP: testClusterIPv6,
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: testExternalIPv4},
							{IP: testExternalIPv6},
						},
					},
				},
			},
		},
		// IPv6 模式下应过滤掉 IPv4 的 ClusterIP。
		{
			desc:     "service with ipv4 clusterIP under ipv6 mode, service should be filtered",
			ipFamily: v1.IPv6Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalidIPv4InIPV6Mode",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP: testClusterIPv4,
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: testExternalIPv4},
							{IP: testExternalIPv6},
						},
					},
				},
			},
		},
		// IPv4 模式下，仅保留 IPv4 的 ExternalIP、LB 源范围和 Ingress IP。
		{
			desc:     "service with ipv4 configurations under ipv4 mode",
			ipFamily: v1.IPv4Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "validIPv4",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP:                testClusterIPv4,
					ExternalIPs:              []string{testExternalIPv4},
					LoadBalancerSourceRanges: []string{testSourceRangeIPv4},
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: testExternalIPv4},
							{IP: testExternalIPv6},
						},
					},
				},
			},
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("test", "validIPv4", "testPort", v1.ProtocolTCP): makeTestServiceInfo(testClusterIPv4, 12345, "TCP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.externalIPs = makeIPs(testExternalIPv4)
					bsvcPortInfo.loadBalancerSourceRanges = mustMakeCIDRs(testSourceRangeIPv4)
					bsvcPortInfo.loadBalancerVIPs = makeIPs(testExternalIPv4)
				}),
			},
		},
		// IPv6 模式下，仅保留 IPv6 地址相关字段。
		{
			desc:     "service with ipv6 configurations under ipv6 mode",
			ipFamily: v1.IPv6Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "validIPv6",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP:                testClusterIPv6,
					ExternalIPs:              []string{testExternalIPv6},
					LoadBalancerSourceRanges: []string{testSourceRangeIPv6},
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: testExternalIPv4},
							{IP: testExternalIPv6},
						},
					},
				},
			},
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("test", "validIPv6", "testPort", v1.ProtocolTCP): makeTestServiceInfo(testClusterIPv6, 12345, "TCP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.externalIPs = makeIPs(testExternalIPv6)
					bsvcPortInfo.loadBalancerSourceRanges = mustMakeCIDRs(testSourceRangeIPv6)
					bsvcPortInfo.loadBalancerVIPs = makeIPs(testExternalIPv6)
				}),
			},
		},
		// 双栈配置下，IPv4 模式必须丢弃所有 IPv6 字段。
		{
			desc:     "service with both ipv4 and ipv6 configurations under ipv4 mode, ipv6 fields should be filtered",
			ipFamily: v1.IPv4Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "filterIPv6InIPV4Mode",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP:                testClusterIPv4,
					ExternalIPs:              []string{testExternalIPv4, testExternalIPv6},
					LoadBalancerSourceRanges: []string{testSourceRangeIPv4, testSourceRangeIPv6},
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: testExternalIPv4},
							{IP: testExternalIPv6},
						},
					},
				},
			},
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("test", "filterIPv6InIPV4Mode", "testPort", v1.ProtocolTCP): makeTestServiceInfo(testClusterIPv4, 12345, "TCP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.externalIPs = makeIPs(testExternalIPv4)
					bsvcPortInfo.loadBalancerSourceRanges = mustMakeCIDRs(testSourceRangeIPv4)
					bsvcPortInfo.loadBalancerVIPs = makeIPs(testExternalIPv4)
				}),
			},
		},
		// 双栈配置下，IPv6 模式必须丢弃所有 IPv4 字段。
		{
			desc:     "service with both ipv4 and ipv6 configurations under ipv6 mode, ipv4 fields should be filtered",
			ipFamily: v1.IPv6Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "filterIPv4InIPV6Mode",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP:                testClusterIPv6,
					ExternalIPs:              []string{testExternalIPv4, testExternalIPv6},
					LoadBalancerSourceRanges: []string{testSourceRangeIPv4, testSourceRangeIPv6},
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: testExternalIPv4},
							{IP: testExternalIPv6},
						},
					},
				},
			},
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("test", "filterIPv4InIPV6Mode", "testPort", v1.ProtocolTCP): makeTestServiceInfo(testClusterIPv6, 12345, "TCP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.externalIPs = makeIPs(testExternalIPv6)
					bsvcPortInfo.loadBalancerSourceRanges = mustMakeCIDRs(testSourceRangeIPv6)
					bsvcPortInfo.loadBalancerVIPs = makeIPs(testExternalIPv6)
				}),
			},
		},
		// CIDR 前后的空白会被解析逻辑清理。
		{
			desc:     "service with extra space in LoadBalancerSourceRanges",
			ipFamily: v1.IPv4Protocol,

			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "extra-space",
					Namespace: "test",
				},
				Spec: v1.ServiceSpec{
					ClusterIP:                testClusterIPv4,
					LoadBalancerSourceRanges: []string{" 10.1.2.0/28"},
					Ports: []v1.ServicePort{
						{
							Name:     "testPort",
							Port:     int32(12345),
							Protocol: v1.ProtocolTCP,
						},
					},
				},
			},
			expected: map[ServicePortName]*BaseServicePortInfo{
				makeServicePortName("test", "extra-space", "testPort", v1.ProtocolTCP): makeTestServiceInfo(testClusterIPv4, 12345, "TCP", 0, func(bsvcPortInfo *BaseServicePortInfo) {
					bsvcPortInfo.loadBalancerSourceRanges = mustMakeCIDRs("10.1.2.0/28")
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// 使用与用例匹配的 IP 族构造 tracker，避免地址族过滤干扰断言。
			svcTracker := NewServiceChangeTracker(tc.ipFamily, nil, nil)
			// 输出：转换后的服务映射。
			// 先核对数量，防止多余条目被逐项检查遗漏。
			newServices := svcTracker.serviceToServiceMap(tc.service)

			if len(newServices) != len(tc.expected) {
				t.Fatalf("expected %d new, got %d: %v", len(tc.expected), len(newServices), dump.Pretty(newServices))
			}
			// 逐字段比较，覆盖 IP 族过滤后的 ExternalIP、LB VIP 等列表。
			for svcKey, expectedInfo := range tc.expected {
				svcInfo, exists := newServices[svcKey].(*BaseServicePortInfo)
				if !exists {
					t.Fatalf("[%s] expected to find key %s", tc.desc, svcKey)
				}

				if !svcInfo.clusterIP.Equal(expectedInfo.clusterIP) ||
					svcInfo.port != expectedInfo.port ||
					svcInfo.protocol != expectedInfo.protocol ||
					svcInfo.healthCheckNodePort != expectedInfo.healthCheckNodePort ||
					!reflect.DeepEqual(svcInfo.externalIPs, expectedInfo.externalIPs) ||
					!reflect.DeepEqual(svcInfo.loadBalancerSourceRanges, expectedInfo.loadBalancerSourceRanges) ||
					!reflect.DeepEqual(svcInfo.loadBalancerVIPs, expectedInfo.loadBalancerVIPs) {
					t.Errorf("[%s] expected new[%v]to be %v, got %v", tc.desc, svcKey, expectedInfo, *svcInfo)
				}
				// 再完整遍历一次期望结果，避免外层断言提前分支导致漏检。
				for svcKey, expectedInfo := range tc.expected {
					svcInfo, _ := newServices[svcKey].(*BaseServicePortInfo)
					if !svcInfo.clusterIP.Equal(expectedInfo.clusterIP) ||
						svcInfo.port != expectedInfo.port ||
						svcInfo.protocol != expectedInfo.protocol ||
						svcInfo.healthCheckNodePort != expectedInfo.healthCheckNodePort ||
						!reflect.DeepEqual(svcInfo.externalIPs, expectedInfo.externalIPs) ||
						!reflect.DeepEqual(svcInfo.loadBalancerSourceRanges, expectedInfo.loadBalancerSourceRanges) ||
						!reflect.DeepEqual(svcInfo.loadBalancerVIPs, expectedInfo.loadBalancerVIPs) {
						t.Errorf("expected new[%v]to be %v, got %v", svcKey, expectedInfo, *svcInfo)
					}
				}
			}
		})
	}
}

// FakeProxier 聚合 change tracker 与映射，供测试驱动增量更新流程。
type FakeProxier struct {
	endpointsChanges *EndpointsChangeTracker
	serviceChanges   *ServiceChangeTracker
	svcPortMap       ServicePortMap
	endpointsMap     EndpointsMap
}

// newFakeProxier 创建测试用 FakeProxier，并可指定 tracker 起始时间。
func newFakeProxier(ipFamily v1.IPFamily, t time.Time) *FakeProxier {
	// 固定 trackerStartTime，使基于时间的判断在测试中可预测。
	ect := NewEndpointsChangeTracker(ipFamily, testHostname, nil, nil)
	ect.trackerStartTime = t
	return &FakeProxier{
		svcPortMap:       make(ServicePortMap),
		serviceChanges:   NewServiceChangeTracker(ipFamily, nil, nil),
		endpointsMap:     make(EndpointsMap),
		endpointsChanges: ect,
	}
}

// makeServiceMap 将多个 Service 批量写入 change tracker。
func makeServiceMap(fake *FakeProxier, allServices ...*v1.Service) {
	for i := range allServices {
		fake.addService(allServices[i])
	}
}

// addService 模拟 Service 新增事件。
func (proxier *FakeProxier) addService(service *v1.Service) {
	proxier.serviceChanges.Update(nil, service)
}

// updateService 模拟 Service 更新事件。
func (proxier *FakeProxier) updateService(oldService *v1.Service, service *v1.Service) {
	proxier.serviceChanges.Update(oldService, service)
}

// deleteService 模拟 Service 删除事件（newService 为 nil）。
func (proxier *FakeProxier) deleteService(service *v1.Service) {
	proxier.serviceChanges.Update(service, nil)
}

// TestServiceMapUpdateHeadless 验证 Headless Service 不会进入 service map。
func TestServiceMapUpdateHeadless(t *testing.T) {
	fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

	// 同时覆盖带端口和不带端口的 Headless Service。
	makeServiceMap(fp,
		makeTestService("ns2", "headless", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = v1.ClusterIPNone
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "rpc", "UDP", 1234, 0, 0)
		}),
		makeTestService("ns2", "headless-without-port", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = v1.ClusterIPNone
		}),
	)

	// Headless Service 没有可代理的 ClusterIP，应被忽略。
	result := fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 0 {
		t.Errorf("expected service map length 0, got %d", len(fp.svcPortMap))
	}

	if len(result.UpdatedServices) != 0 {
		t.Errorf("expected 0 updated services, got %d", len(result.UpdatedServices))
	}

	// 没有被代理的 Service，因此不应产生健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %d", len(healthCheckNodePorts))
	}
}

// TestUpdateServiceTypeExternalName 验证 ExternalName Service 不会被代理。
func TestUpdateServiceTypeExternalName(t *testing.T) {
	fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

	// 即使设置了 ClusterIP 与端口，ExternalName Service 也应被跳过。
	makeServiceMap(fp,
		makeTestService("ns2", "external-name", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeExternalName
			svc.Spec.ClusterIP = "172.16.55.4" // 应被忽略：ExternalName 类型不使用 ClusterIP
			svc.Spec.ExternalName = "foo2.bar.com"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "blah", "UDP", 1235, 5321, 0)
		}),
	)

	result := fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 0 {
		t.Errorf("expected service map length 0, got %v", fp.svcPortMap)
	}
	if len(result.UpdatedServices) != 0 {
		t.Errorf("expected 0 updated services, got %v", result.UpdatedServices)
	}

	// 没有被代理的 Service，因此不应产生健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}
}

// TestBuildServiceMapAddRemove 验证 Service 的批量添加、修改与删除。
func TestBuildServiceMapAddRemove(t *testing.T) {
	fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

	// 准备多种类型的 Service，覆盖映射与删除路径。
	services := []*v1.Service{
		makeTestService("ns2", "cluster-ip", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = "172.16.55.4"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port1", "UDP", 1234, 4321, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port2", "UDP", 1235, 5321, 0)
		}),
		makeTestService("ns2", "node-port", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.ClusterIP = "172.16.55.10"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port1", "UDP", 345, 678, 0)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "port2", "TCP", 344, 677, 0)
		}),
		makeTestService("ns1", "load-balancer", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = "172.16.55.11"
			svc.Spec.LoadBalancerIP = "5.6.7.8"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "foobar", "UDP", 8675, 30061, 7000)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "baz", "UDP", 8676, 30062, 7001)
			svc.Status.LoadBalancer = v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "10.1.2.4"},
				},
			}
		}),
		makeTestService("ns1", "only-local-load-balancer", func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = "172.16.55.12"
			svc.Spec.LoadBalancerIP = "5.6.7.8"
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "foobar2", "UDP", 8677, 30063, 7002)
			svc.Spec.Ports = addTestPort(svc.Spec.Ports, "baz", "UDP", 8678, 30064, 7003)
			svc.Status.LoadBalancer = v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{
					{IP: "10.1.2.3"},
				},
			}
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.HealthCheckNodePort = 345
		}),
	}

	// 先批量添加所有 Service。
	for i := range services {
		fp.addService(services[i])
	}

	// 首次 Update 应把所有新增 Service 都报告为已更新。
	result := fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 8 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	// 每个 Service 都应出现在 UpdatedServices 中。
	for i := range services {
		name := makeNSN(services[i].Namespace, services[i].Name)
		if !result.UpdatedServices.Has(name) {
			t.Errorf("expected updated service for %q", name)
		}
	}
	if len(result.UpdatedServices) != len(services) {
		t.Errorf("expected %d updated services, got %d", len(services), len(result.UpdatedServices))
	}

	// 只有 ExternalTrafficPolicy=Local 的 LoadBalancer Service 才需要健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	// 删除后不应再有健康检查端口残留。
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected 1 healthcheck port, got %v", healthCheckNodePorts)
	} else {
		nsn := makeNSN("ns1", "only-local-load-balancer")
		if port, found := healthCheckNodePorts[nsn]; !found || port != 345 {
			t.Errorf("expected healthcheck port [%q]=345: got %v", nsn, healthCheckNodePorts)
		}
	}

	// 删除部分 Service 并修改其中一个，验证映射能收敛到正确状态。
	// oneService 基于 services[0] 构造，只移除了第一个端口。
	oneService := makeTestService("ns2", "cluster-ip", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.16.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p2", "UDP", 1235, 5321, 0)
	})

	// 修改一个 Service 的端口，并删除其余三个。
	fp.updateService(services[0], oneService)
	fp.deleteService(services[1])
	fp.deleteService(services[2])
	fp.deleteService(services[3])

	// 第二次 Update 只反映实际发生的变更。
	result = fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 1 {
		t.Errorf("expected service map length 1, got %v", fp.svcPortMap)
	}
	if len(result.UpdatedServices) != 4 {
		t.Errorf("expected 4 updated services, got %d", len(result.UpdatedServices))
	}

	// 删除 Local 策略的 LoadBalancer 后，健康检查端口应清空。
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected 0 healthcheck ports, got %v", healthCheckNodePorts)
	}
}

// TestBuildServiceMapServiceUpdate 验证 Service 类型变更时映射与健康检查端口的同步。
func TestBuildServiceMapServiceUpdate(t *testing.T) {
	fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

	// 准备同一 Service 的两个版本：ClusterIP 与仅本地 LoadBalancer。
	servicev1 := makeTestService("ns1", "svc1", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.16.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p1", "UDP", 1234, 4321, 0)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p2", "TCP", 1235, 5321, 0)
	})
	servicev2 := makeTestService("ns1", "svc1", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeLoadBalancer
		svc.Spec.ClusterIP = "172.16.55.4"
		svc.Spec.LoadBalancerIP = "5.6.7.8"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p1", "UDP", 1234, 4321, 7002)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p2", "TCP", 1235, 5321, 7003)
		svc.Status.LoadBalancer = v1.LoadBalancerStatus{
			Ingress: []v1.LoadBalancerIngress{
				{IP: "10.1.2.3"},
			},
		}
		svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		svc.Spec.HealthCheckNodePort = 345
	})

	// 初始状态为 ClusterIP 类型。
	fp.addService(servicev1)

	// 首次 Update 建立两个端口的映射。
	result := fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	if len(result.UpdatedServices) != 1 {
		t.Errorf("expected 1 updated service, got %d", len(result.UpdatedServices))
	}

	// ClusterIP 类型没有健康检查端口。
	healthCheckNodePorts := fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}

	// 将 Service 改为 LoadBalancer 类型，观察健康检查端口是否出现。
	fp.updateService(servicev1, servicev2)
	// 更新后端口数不变，但应新增健康检查端口。
	result = fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	if len(result.UpdatedServices) != 1 {
		t.Errorf("expected 1 updated service, got %d", len(result.UpdatedServices))
	}

	// Local 流量策略下应出现健康检查端口。
	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected healthcheck ports length 1, got %v", healthCheckNodePorts)
	}

	// 无实际变更时，service map 与健康检查端口都应保持不变；
	// 这里用相同对象再次 Update 来模拟反复同步的场景。
	fp.updateService(servicev2, servicev2)
	// 无变更的 Update 不应报告任何更新。
	result = fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	if len(result.UpdatedServices) != 0 {
		t.Errorf("expected 0 updated services, got %d", len(result.UpdatedServices))
	}

	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 1 {
		t.Errorf("expected healthcheck ports length 1, got %v", healthCheckNodePorts)
	}

	// 再改回 ClusterIP 类型，确认健康检查端口随之移除。
	fp.updateService(servicev2, servicev1)
	// 回到 ClusterIP 后健康检查端口应消失。
	result = fp.svcPortMap.Update(fp.serviceChanges)
	if len(fp.svcPortMap) != 2 {
		t.Errorf("expected service map length 2, got %v", fp.svcPortMap)
	}
	if len(result.UpdatedServices) != 1 {
		t.Errorf("expected 1 updated service, got %d", len(result.UpdatedServices))
	}

	healthCheckNodePorts = fp.svcPortMap.HealthCheckNodePorts()
	if len(healthCheckNodePorts) != 0 {
		t.Errorf("expected healthcheck ports length 0, got %v", healthCheckNodePorts)
	}
}

// TestServiceCacheLeaks 验证 Service 删除后 change tracker 缓存不会保留条目。
func TestServiceCacheLeaks(t *testing.T) {
	fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

	service := makeTestService("ns1", "svc1", func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "172.16.55.4"
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p1", "UDP", 1234, 4321, 0)
		svc.Spec.Ports = addTestPort(svc.Spec.Ports, "p2", "TCP", 1235, 5321, 0)
	})
	// 添加后缓存中应恰好有一个条目。
	fp.addService(service)
	if len(fp.serviceChanges.items) != 1 {
		t.Errorf("Found %d items on the cache, 1 expected", len(fp.serviceChanges.items))
	}

	// 删除后缓存应被清空，避免长期运行时发生内存泄漏。
	fp.deleteService(service)
	if len(fp.serviceChanges.items) > 0 {
		t.Errorf("Found %d items on the cache, 0 expected", len(fp.serviceChanges.items))
	}
}
