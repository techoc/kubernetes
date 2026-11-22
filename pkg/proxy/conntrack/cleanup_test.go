//go:build linux

/*
Copyright 2023 The Kubernetes Authors.

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

package conntrack

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

const (
	testServiceName      = "cleanup-test"
	testServiceNamespace = "test"

	testIPFamily       = v1.IPv4Protocol
	testClusterIP      = "172.30.1.1"
	testExternalIP     = "192.168.99.100"
	testLoadBalancerIP = "1.2.3.4"

	testServingEndpointIP    = "10.240.0.4"
	testNonServingEndpointIP = "10.240.1.5"
	testDeletedEndpointIP    = "10.240.2.6"

	// testOldEndpointPort 用于覆盖端点端口变化但 IP 不变的场景。
	testOldEndpointPort = 8080
	testEndpointPort    = 9090
	testServicePort     = 8000
	testServiceNodePort = 32000
	// testNonServicePort 用于模拟不属于 kube-proxy 管理的 conntrack 表项，
	// 这些表项不应被清理器纳入回收范围。
	testNonServicePort = 3000
)

// generateConntrackEntry 构造单测用的 *netlink.ConntrackFlow。
func generateConntrackEntry(origDst string, origPortDst uint16, replySrc string, replyPortSrc uint16, proto uint8) *netlink.ConntrackFlow {
	entry := &netlink.ConntrackFlow{
		FamilyType: unix.AF_INET,
		Forward: netlink.IPTuple{
			DstPort:  origPortDst,
			Protocol: proto,
		},
		Reverse: netlink.IPTuple{
			Protocol: proto,
			SrcIP:    netutils.ParseIPSloppy(replySrc),
			SrcPort:  replyPortSrc,
		},
	}
	// NodePort 服务只按目的端口匹配，不按 --orig-dst 匹配，因此这里单独处理 origDst。
	if origDst != "" {
		entry.Forward.DstIP = netutils.ParseIPSloppy(origDst)
	}
	return entry
}

// TestCleanStaleEntries 验证清理器删除过期 conntrack 表项、保留合法表项并上报删除计数。
func TestCleanStaleEntries(t *testing.T) {
	// 需要构造 proxy.ServicePortMap 和 proxy.EndpointsMap 传给 CleanStaleEntries。
	// 这两个类型本质上是 map，但 proxy.ServicePort 等接口没有公开的构造入口，
	// 因此借助 proxy.ServiceChangeTracker 和 proxy.NewEndpointsChangeTracker
	// 生成实现并填充映射，而不是自己实现一套接口。

	sct := proxy.NewServiceChangeTracker(v1.IPv4Protocol, nil, nil)
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testServiceName,
			Namespace: testServiceNamespace,
		},
		Spec: v1.ServiceSpec{
			ClusterIP:   testClusterIP,
			ExternalIPs: []string{testExternalIP},
			Ports: []v1.ServicePort{
				{
					Name:     "test-tcp",
					Port:     testServicePort,
					Protocol: v1.ProtocolTCP,
				},
				{
					Name:     "test-udp",
					Port:     testServicePort,
					NodePort: testServiceNodePort,
					Protocol: v1.ProtocolUDP,
				},
				{
					Name:     "test-sctp",
					Port:     testServicePort,
					NodePort: testServiceNodePort,
					Protocol: v1.ProtocolSCTP,
				},
			},
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{{
					IP: testLoadBalancerIP,
				}},
			},
		},
	}

	sct.Update(nil, svc)
	svcPortMap := make(proxy.ServicePortMap)
	_ = svcPortMap.Update(sct)

	ect := proxy.NewEndpointsChangeTracker(v1.IPv4Protocol, "test-worker", nil, nil)
	eps := &discovery.EndpointSlice{
		TypeMeta:    metav1.TypeMeta{},
		AddressType: discovery.AddressTypeIPv4,
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-0", testServiceName),
			Namespace: testServiceNamespace,
			Labels:    map[string]string{discovery.LabelServiceName: testServiceName},
		},
		Endpoints: []discovery.Endpoint{
			{
				Addresses:  []string{testServingEndpointIP},
				Conditions: discovery.EndpointConditions{Serving: ptr.To(true)},
			},
			{
				Addresses:  []string{testNonServingEndpointIP},
				Conditions: discovery.EndpointConditions{Serving: ptr.To(false)},
			},
		},
		Ports: []discovery.EndpointPort{
			{
				Name:     ptr.To("test-tcp"),
				Port:     ptr.To(int32(testEndpointPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			},
			{
				Name:     ptr.To("test-udp"),
				Port:     ptr.To(int32(testEndpointPort)),
				Protocol: ptr.To(v1.ProtocolUDP),
			},
			{
				Name:     ptr.To("test-sctp"),
				Port:     ptr.To(int32(testEndpointPort)),
				Protocol: ptr.To(v1.ProtocolSCTP),
			},
		},
	}

	ect.EndpointSliceUpdate(eps, false)
	endpointsMap := make(proxy.EndpointsMap)
	_ = endpointsMap.Update(ect)

	tcpPortName := proxy.ServicePortName{
		NamespacedName: types.NamespacedName{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		},
		Port:     svc.Spec.Ports[0].Name,
		Protocol: svc.Spec.Ports[0].Protocol,
	}

	udpPortName := proxy.ServicePortName{
		NamespacedName: types.NamespacedName{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		},
		Port:     svc.Spec.Ports[1].Name,
		Protocol: svc.Spec.Ports[1].Protocol,
	}

	sctpPortName := proxy.ServicePortName{
		NamespacedName: types.NamespacedName{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		},
		Port:     svc.Spec.Ports[2].Name,
		Protocol: svc.Spec.Ports[2].Protocol,
	}

	// 先确认 ServicePortMap 构造正确，避免把构造错误误判为清理逻辑的错误。
	if len(svcPortMap) != 3 {
		t.Fatalf("expected svcPortMap to have 2 entries, got %+v", svcPortMap)
	}
	servicePort := svcPortMap[tcpPortName]
	if servicePort == nil || servicePort.String() != "172.30.1.1:8000/TCP" {
		t.Fatalf("expected svcPortMap[%q] to be \"172.30.1.1:8000/TCP\", got %q", tcpPortName.String(), servicePort.String())
	}
	servicePort = svcPortMap[udpPortName]
	if servicePort == nil || servicePort.String() != "172.30.1.1:8000/UDP" {
		t.Fatalf("expected svcPortMap[%q] to be \"172.30.1.1:8000/UDP\", got %q", udpPortName.String(), servicePort.String())
	}
	servicePort = svcPortMap[sctpPortName]
	if servicePort == nil || servicePort.String() != "172.30.1.1:8000/SCTP" {
		t.Fatalf("expected svcPortMap[%q] to be \"172.30.1.1:8000/SCTP\", got %q", sctpPortName.String(), servicePort.String())
	}

	// 同理确认 EndpointsMap 的数量、IP 与就绪状态符合预期。
	if len(endpointsMap) != 3 {
		t.Fatalf("expected endpointsMap to have 3 entries, got %+v", endpointsMap)
	}
	for _, svcPortName := range []proxy.ServicePortName{tcpPortName, udpPortName, sctpPortName} {
		if len(endpointsMap[svcPortName]) != 2 {
			t.Fatalf("expected endpointsMap[%q] to have 2 entries, got %+v", svcPortName.String(), endpointsMap[svcPortName])
		}
		if endpointsMap[svcPortName][0].IP() != "10.240.0.4" {
			t.Fatalf("expected endpointsMap[%q][0] IP to be \"10.240.0.4\", got \"%s\"", svcPortName.String(), endpointsMap[svcPortName][0].IP())
		}
		if endpointsMap[svcPortName][1].IP() != "10.240.1.5" {
			t.Fatalf("expected endpointsMap[%q][1] IP to be \"10.240.1.5\", got \"%s\"", svcPortName.String(), endpointsMap[svcPortName][1].IP())
		}
		if !endpointsMap[svcPortName][0].IsServing() {
			t.Fatalf("expected endpointsMap[%q][0] to be serving", svcPortName.String())
		}
		if endpointsMap[svcPortName][1].IsServing() {
			t.Fatalf("expected endpointsMap[%q][1] to be not serving", svcPortName.String())
		}
	}

	// entriesBeforeCleanup 与 entriesAfterCleanup 分别表示清理前后的 conntrack 表项快照：
	// 清理器读取前者，测试断言清理后的实际表项与后者一致。
	// 二者之差即为清理器应删除的表项。
	var entriesBeforeCleanup []*netlink.ConntrackFlow
	var entriesAfterCleanup []*netlink.ConntrackFlow

	// 构造 6 条旧端口表项，模拟端点 IP 不变但端口变化的场景，这些表项应被清理。
	for _, origDest := range []string{testClusterIP, testLoadBalancerIP, testExternalIP} {
		entry := generateConntrackEntry(origDest, testServicePort, testServingEndpointIP, testOldEndpointPort, unix.IPPROTO_UDP)
		entriesBeforeCleanup = append(entriesBeforeCleanup, entry)
	}

	// 构造 63 条表项：3 个端点 × 3 种协议 × (3 条 ServiceIP:ServicePort + 3 条 ServiceIP:非服务端口 + 1 条 NodePort)。
	for _, dnatDest := range []string{testServingEndpointIP, testNonServingEndpointIP, testDeletedEndpointIP} {
		for _, proto := range []uint8{unix.IPPROTO_TCP, unix.IPPROTO_UDP, unix.IPPROTO_SCTP} {
			for _, origDest := range []string{testClusterIP, testLoadBalancerIP, testExternalIP} {
				for _, port := range []uint16{testServicePort, testNonServicePort} {
					entry := generateConntrackEntry(origDest, port, dnatDest, testEndpointPort, proto)
					entriesBeforeCleanup = append(entriesBeforeCleanup, entry)
					if proto == unix.IPPROTO_UDP && port == testServicePort && dnatDest != testServingEndpointIP {
						// UDP 表项目的是 testServicePort 但 DNAT 目标不是就绪端点时，清理后不应保留。
					} else {
						entriesAfterCleanup = append(entriesAfterCleanup, entry)
					}
				}
			}

			entry := generateConntrackEntry("", testServiceNodePort, dnatDest, testEndpointPort, proto)
			entriesBeforeCleanup = append(entriesBeforeCleanup, entry)
			if proto == unix.IPPROTO_UDP && dnatDest != testServingEndpointIP {
				// DNAT 目标不是就绪端点的 UDP NodePort 表项，清理后同样不应保留。
			} else {
				entriesAfterCleanup = append(entriesAfterCleanup, entry)
			}
		}
	}

	// 补充 6 条未做 DNAT 的表项，这些也应被清理：
	// 服务没有端点且代理缺少 DROP/REJECT 规则时会留下这类表项，其 --orig-dst 与 --reply-src 相同。
	for _, ip := range []string{testClusterIP, testLoadBalancerIP, testExternalIP} {
		for _, port := range []uint16{testServicePort, testNonServicePort} {
			entry := &netlink.ConntrackFlow{
				FamilyType: unix.AF_INET,
				Forward: netlink.IPTuple{
					DstIP:    netutils.ParseIPSloppy(ip),
					DstPort:  port,
					Protocol: unix.IPPROTO_UDP,
				},
				Reverse: netlink.IPTuple{
					Protocol: unix.IPPROTO_UDP,
					SrcIP:    netutils.ParseIPSloppy(ip),
				},
			}

			entriesBeforeCleanup = append(entriesBeforeCleanup, entry)
			// 目的端口为 testServicePort 的表项属于服务流量，清理后不应保留。
			if port != testServicePort {
				entriesAfterCleanup = append(entriesAfterCleanup, entry)
			}

		}
	}

	t.Logf("entries before cleanup %d after cleanup %d", len(entriesBeforeCleanup), len(entriesAfterCleanup))
	ct := newConntracker(
		&fakeHandler{
			entries: entriesBeforeCleanup,
		},
	)

	legacyregistry.MustRegister(metrics.ReconcileConntrackFlowsDeletedEntriesTotal)
	CleanStaleEntries(ct, testIPFamily, svcPortMap, endpointsMap)
	actualEntries, _ := ct.ListEntries(ipFamilyMap[testIPFamily])

	metricCount, err := testutil.GetCounterMetricValue(metrics.ReconcileConntrackFlowsDeletedEntriesTotal.WithLabelValues(string(testIPFamily)))
	require.NoError(t, err)
	require.Equal(t, int(metricCount), len(entriesBeforeCleanup)-len(entriesAfterCleanup))

	require.Equal(t, len(entriesAfterCleanup), len(actualEntries))

	// 表项顺序不确定，比较前先排序。
	sort.Slice(actualEntries, func(i, j int) bool {
		return actualEntries[i].String() < actualEntries[j].String()
	})
	// 期望表项同样排序，保证 diff 不受插入顺序影响。
	sort.Slice(entriesAfterCleanup, func(i, j int) bool {
		return entriesAfterCleanup[i].String() < entriesAfterCleanup[j].String()
	})

	if diff := cmp.Diff(entriesAfterCleanup, actualEntries); len(diff) > 0 {
		t.Errorf("unexpected entries after cleanup: %s", diff)
	}
}

// TestPerformanceCleanStaleEntries 验证百万级表项下清理请求数只随过期表项增长，不随表规模爆炸。
func TestPerformanceCleanStaleEntries(t *testing.T) {
	sct := proxy.NewServiceChangeTracker(v1.IPv4Protocol, nil, nil)
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testServiceName,
			Namespace: testServiceNamespace,
		},
		Spec: v1.ServiceSpec{
			ClusterIP:   testClusterIP,
			ExternalIPs: []string{testExternalIP},
			Ports: []v1.ServicePort{
				{
					Name:     "test-udp",
					Port:     testServicePort,
					Protocol: v1.ProtocolUDP,
				},
			},
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{{
					IP: testLoadBalancerIP,
				}},
			},
		},
	}

	sct.Update(nil, svc)
	svcPortMap := make(proxy.ServicePortMap)
	_ = svcPortMap.Update(sct)

	ect := proxy.NewEndpointsChangeTracker(v1.IPv4Protocol, "test-worker", nil, nil)
	eps := &discovery.EndpointSlice{
		TypeMeta:    metav1.TypeMeta{},
		AddressType: discovery.AddressTypeIPv4,
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-0", testServiceName),
			Namespace: testServiceNamespace,
			Labels:    map[string]string{discovery.LabelServiceName: testServiceName},
		},
		Endpoints: []discovery.Endpoint{
			{
				Addresses:  []string{testServingEndpointIP},
				Conditions: discovery.EndpointConditions{Serving: ptr.To(true)},
			},
			{
				Addresses:  []string{testNonServingEndpointIP},
				Conditions: discovery.EndpointConditions{Serving: ptr.To(false)},
			},
		},
		Ports: []discovery.EndpointPort{
			{
				Name:     ptr.To("test-udp"),
				Port:     ptr.To(int32(testEndpointPort)),
				Protocol: ptr.To(v1.ProtocolUDP),
			},
		},
	}

	ect.EndpointSliceUpdate(eps, false)
	endpointsMap := make(proxy.EndpointsMap)
	_ = endpointsMap.Update(ect)

	flows := []*netlink.ConntrackFlow{}
	// 1 条合法表项。
	flows = append(flows, generateConntrackEntry(testExternalIP, testServicePort, testServingEndpointIP, testEndpointPort, unix.IPPROTO_UDP))
	expectedEntries := 1
	// 1 条过期表项。
	flows = append(flows, generateConntrackEntry(testExternalIP, testServicePort, testDeletedEndpointIP, testEndpointPort, unix.IPPROTO_UDP))
	expectedDeleted := 1
	// 构造百万条随源端口变化的表项，用于压测清理路径的开销。
	for i := 0; i < 1000*1000; i++ {
		port := uint16(rand.Intn(65535))
		if port == testServicePort {
			expectedDeleted++
		} else {
			expectedEntries++
		}
		flows = append(flows, generateConntrackEntry(testExternalIP, port, testDeletedEndpointIP, testEndpointPort, unix.IPPROTO_UDP))
	}

	fake := &fakeHandler{entries: flows}
	ct := newConntracker(fake)

	CleanStaleEntries(ct, testIPFamily, svcPortMap, endpointsMap)
	actualEntries, _ := ct.ListEntries(ipFamilyMap[testIPFamily])
	if len(actualEntries) != expectedEntries {
		t.Errorf("unexpected number of entries, got %d expected %d", len(actualEntries), expectedEntries)
	}
	// 预期 netlink 请求数：1 次 CleanStaleEntries 列表 + 1 次 ListEntries + 1 次 ConntrackDeleteFilters dump，
	// 再加上每条待删除过期表项各一次删除请求。
	t.Logf("expected deleted %d", expectedDeleted)
	if fake.netlinkRequests != 3+expectedDeleted {
		t.Errorf("expected %d netlink requests, got %d", 3+expectedDeleted, fake.netlinkRequests)
	}
}

// TestServiceWithoutEndpoints 验证服务无就绪端点时的清理边界：只删除自有 UDP 前端表项，保留其余表项。
func TestServiceWithoutEndpoints(t *testing.T) {
	sct := proxy.NewServiceChangeTracker(v1.IPv4Protocol, nil, nil)
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testServiceName,
			Namespace: testServiceNamespace,
		},
		Spec: v1.ServiceSpec{
			ClusterIP:   testClusterIP,
			ExternalIPs: []string{testExternalIP},
			Ports: []v1.ServicePort{
				{
					Name:     "test-udp",
					Port:     testServicePort,
					NodePort: testServiceNodePort,
					Protocol: v1.ProtocolUDP,
				},
			},
		},
		Status: v1.ServiceStatus{
			LoadBalancer: v1.LoadBalancerStatus{
				Ingress: []v1.LoadBalancerIngress{{
					IP: testLoadBalancerIP,
				}},
			},
		},
	}

	sct.Update(nil, svc)
	svcPortMap := make(proxy.ServicePortMap)
	_ = svcPortMap.Update(sct)

	ect := proxy.NewEndpointsChangeTracker(v1.IPv4Protocol, "test-worker", nil, nil)
	eps := &discovery.EndpointSlice{
		TypeMeta:    metav1.TypeMeta{},
		AddressType: discovery.AddressTypeIPv4,
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-0", testServiceName),
			Namespace: testServiceNamespace,
			Labels:    map[string]string{discovery.LabelServiceName: "non-existing-service"},
		},
		Endpoints: []discovery.Endpoint{
			{
				Addresses:  []string{testServingEndpointIP},
				Conditions: discovery.EndpointConditions{Serving: ptr.To(true)},
			},
			{
				Addresses:  []string{testNonServingEndpointIP},
				Conditions: discovery.EndpointConditions{Serving: ptr.To(false)},
			},
		},
		Ports: []discovery.EndpointPort{
			{
				Name:     ptr.To("test-udp"),
				Port:     ptr.To(int32(testEndpointPort)),
				Protocol: ptr.To(v1.ProtocolUDP),
			},
		},
	}

	ect.EndpointSliceUpdate(eps, false)
	endpointsMap := make(proxy.EndpointsMap)
	_ = endpointsMap.Update(ect)

	flows := []*netlink.ConntrackFlow{}
	var entriesAfterCleanup []*netlink.ConntrackFlow

	// 无就绪端点的服务，其指向前端的 UDP 表项都是过期的，无论 DNAT 到哪个已删除端点都应清理。
	// 已建立的连接会绕过为无端点服务安装的 REJECT 规则，否则会持续把流量黑洞到已删除的端点 IP。
	for _, origDest := range []string{testClusterIP, testLoadBalancerIP, testExternalIP} {
		for _, dnatDest := range []string{testServingEndpointIP, testDeletedEndpointIP} {
			flows = append(flows, generateConntrackEntry(origDest, testServicePort, dnatDest, testEndpointPort, unix.IPPROTO_UDP))
		}
	}

	// 不指向服务前端的 UDP 表项不属于 kube-proxy，必须保留。
	entry := generateConntrackEntry(testExternalIP, testNonServicePort, testDeletedEndpointIP, testEndpointPort, unix.IPPROTO_UDP)
	flows = append(flows, entry)
	entriesAfterCleanup = append(entriesAfterCleanup, entry)

	// 非 UDP 表项不在清理范围内，必须保留。
	entry = generateConntrackEntry(testExternalIP, testServicePort, testDeletedEndpointIP, testEndpointPort, unix.IPPROTO_TCP)
	flows = append(flows, entry)
	entriesAfterCleanup = append(entriesAfterCleanup, entry)

	// *:NodePort 表项只按目的端口匹配；端点为空时按此规则清理会误删非 kube-proxy 表项，
	// 因此无就绪端点时跳过 NodePort 清理，这类表项必须保留。
	entry = generateConntrackEntry("", testServiceNodePort, testDeletedEndpointIP, testEndpointPort, unix.IPPROTO_UDP)
	flows = append(flows, entry)
	entriesAfterCleanup = append(entriesAfterCleanup, entry)

	ct := newConntracker(
		&fakeHandler{
			entries: flows,
		},
	)

	CleanStaleEntries(ct, testIPFamily, svcPortMap, endpointsMap)
	actualEntries, _ := ct.ListEntries(ipFamilyMap[testIPFamily])

	require.Len(t, actualEntries, len(entriesAfterCleanup))

	// 表项顺序不确定，比较前先排序。
	sort.Slice(actualEntries, func(i, j int) bool {
		return actualEntries[i].String() < actualEntries[j].String()
	})
	// 期望表项同样排序，保证 diff 不受插入顺序影响。
	sort.Slice(entriesAfterCleanup, func(i, j int) bool {
		return entriesAfterCleanup[i].String() < entriesAfterCleanup[j].String()
	})

	if diff := cmp.Diff(entriesAfterCleanup, actualEntries); len(diff) > 0 {
		t.Errorf("unexpected entries after cleanup: %s", diff)
	}
}
