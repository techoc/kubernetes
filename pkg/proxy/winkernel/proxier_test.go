//go:build windows

/*
Copyright 2021 The Kubernetes Authors.

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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/Microsoft/hnslib/hcn"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/proxy"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/kubernetes/pkg/proxy/healthcheck"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	fakehcn "k8s.io/kubernetes/pkg/proxy/winkernel/testing"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// 测试用固定常量：节点名、网络名、前缀长度、MAC 地址及各网关端点标识，供各测试场景共享。
const (
	testNodeName      = "test-node"
	testNetwork       = "TestNetwork"
	prefixLen         = 24
	macAddress        = "00-11-22-33-44-55"
	macAddressLocal1  = "00-11-22-33-44-56"
	macAddressLocal2  = "00-11-22-33-44-57"
	destinationPrefix = "192.168.2.0/24"
	providerAddress   = "10.0.0.3"
	guid              = "123ABC"
	networkId         = "123ABC"
	endpointGuid1     = "EPID-1"
	emptyLbID         = ""
	loadbalancerGuid1 = "LBID-1"
	loadbalancerGuid2 = "LBID-2"
	endpointLocal1    = "EP-LOCAL-1"
	endpointLocal2    = "EP-LOCAL-2"
	endpointGw        = "EP-GW"
	epIpAddressGw     = "192.168.2.1"
	epIpAddressGwv6   = "192::1"
	epMacAddressGw    = "00-11-22-33-44-66"
)

// 双栈测试按创建顺序使用的负载均衡器 ID 列表。
var (
	loadbalancerGuids = []string{emptyLbID, "LBID-1", "LBID-2", "LBID-3", "LBID-4", "LBID-5", "LBID-6", "LBID-7", "LBID-8"}
)

// newHnsNetwork 根据 hnsNetworkInfo 构造模拟的 HostComputeNetwork，
// 将每个远端子网转换为 RemoteSubnetRoute 类型的网络策略。
func newHnsNetwork(networkInfo *hnsNetworkInfo) *hcn.HostComputeNetwork {
	var policies []hcn.NetworkPolicy
	for _, remoteSubnet := range networkInfo.remoteSubnets {
		policySettings := hcn.RemoteSubnetRoutePolicySetting{
			DestinationPrefix:           remoteSubnet.destinationPrefix,
			IsolationId:                 remoteSubnet.isolationID,
			ProviderAddress:             remoteSubnet.providerAddress,
			DistributedRouterMacAddress: remoteSubnet.drMacAddress,
		}
		settings, _ := json.Marshal(policySettings)
		policy := hcn.NetworkPolicy{
			Type:     hcn.RemoteSubnetRoute,
			Settings: settings,
		}
		policies = append(policies, policy)
	}

	network := &hcn.HostComputeNetwork{
		Id:       networkInfo.id,
		Name:     networkInfo.name,
		Type:     hcn.NetworkType(networkInfo.networkType),
		Policies: policies,
	}
	return network
}

// NewFakeProxier 创建一个基于 HCN mock 的测试用 Proxier。
// 默认使用 IPv4 与指定网络类型，并可按需开启 DSR、指定 IP 家族。
func NewFakeProxier(t *testing.T, nodeName string, nodeIP net.IP, networkType string, enableDSR bool, ipFamily ...v1.IPFamily) *Proxier {
	sourceVip := "192.168.1.2"

	// 如果未提供 ipFamily，则默认使用 IPv4
	family := v1.IPv4Protocol
	if len(ipFamily) > 0 {
		family = ipFamily[0]
	}

	if family == v1.IPv6Protocol {
		sourceVip = "192::1:2"
	}

	// 启用 `WinDSR` 特性门控
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.WinDSR, true)

	config := kubeproxyconfig.KubeProxyWinkernelConfiguration{
		SourceVip:             sourceVip,
		EnableDSR:             enableDSR,
		NetworkName:           testNetwork,
		ForwardHealthCheckVip: true,
	}

	hcnMock := getHcnMock(networkType)

	// 注入 mock HCN 与固定 MAC 地址，避免测试依赖真实的主机网络状态。
	proxier, _ := newProxierInternal(
		family,
		nodeName,
		nodeIP,
		healthcheck.NewFakeServiceHealthServer(),
		nil,
		0,
		hcnMock,
		&testHostMacProvider{macAddress: macAddress},
		config,
		false,
	)

	return proxier
}

// getHcnMock 创建带有一个预设远端子网的 HCN mock，
// 使测试无需依赖真实 Windows HNS 即可查询网络信息。
func getHcnMock(networkType string) *fakehcn.HcnMock {
	var remoteSubnets []*remoteSubnetInfo
	rs := &remoteSubnetInfo{
		destinationPrefix: destinationPrefix,
		isolationID:       4096,
		providerAddress:   providerAddress,
		drMacAddress:      macAddress,
	}
	remoteSubnets = append(remoteSubnets, rs)
	hnsNetworkInfo := &hnsNetworkInfo{
		id:            strings.ToUpper(guid),
		name:          testNetwork,
		networkType:   networkType,
		remoteSubnets: remoteSubnets,
	}
	hnsNetwork := newHnsNetwork(hnsNetworkInfo)
	hcnMock := fakehcn.NewHcnMock(hnsNetwork)
	return hcnMock
}

// TestCreateServiceVip 验证服务创建后 ClusterIP 被记录为 VIP（远端端点）且 serviceInfo 正确。
func TestCreateServiceVip(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcExternalIPs := "50.60.70.81"
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalIPs = []string{svcExternalIPs}
			svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP
			svc.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
				ClientIP: &v1.ClientIPConfig{
					TimeoutSeconds: ptr.To[int32](v1.DefaultClientIPServiceAffinitySeconds),
				},
			}
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	// 先标记已初始化，否则 syncProxyRules 会跳过规则同步。
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		if svcInfo.remoteEndpoint == nil {
			t.Error()
		}
		if svcInfo.remoteEndpoint.ip != svcIP {
			t.Error()
		}
	}
}

// TestCreateRemoteEndpointOverlay 验证 overlay 网络中远端端点的 HNS ID 分配与引用计数初始化。
func TestCreateRemoteEndpointOverlay(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, false)
	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName.String())

	} else {
		if epInfo.hnsID != "EPID-3" {
			t.Errorf("%v does not match %v", epInfo.hnsID, endpointGuid1)
		}
	}

	if *proxier.endPointsRefCount["EPID-3"] <= 0 {
		t.Errorf("RefCount not incremented. Current value: %v", *proxier.endPointsRefCount[endpointGuid1])
	}

	if *proxier.endPointsRefCount["EPID-3"] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}
}

// TestCreateRemoteEndpointL2Bridge 验证 L2Bridge 网络中远端端点的 HNS ID 与引用计数初始化。
func TestCreateRemoteEndpointL2Bridge(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), "L2Bridge", false)
	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	proxier.setInitialized(true)
	proxier.syncProxyRules()
	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName.String())

	} else {
		if epInfo.hnsID != endpointGuid1 {
			t.Errorf("%v does not match %v", epInfo.hnsID, endpointGuid1)
		}
	}

	if *proxier.endPointsRefCount[endpointGuid1] <= 0 {
		t.Errorf("RefCount not incremented. Current value: %v", *proxier.endPointsRefCount[endpointGuid1])
	}

	if *proxier.endPointsRefCount[endpointGuid1] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}
}

// TestDsrEndpointsAreCreatedCorrectly 验证开启 DSR 时端点能被正确创建并获得 HNS ID。
func TestDsrEndpointsAreCreatedCorrectly(t *testing.T) {
	proxier := NewFakeProxier(t, "testhost", netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Fatal("Failed to create proxier")
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "ClusterIP"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName.String())
	}

	if epInfo.hnsID == "" {
		t.Errorf("Expected HNS ID to be set for endpoint %s, but got empty value", epIpAddressRemote)
	}
}

// TestDsrNotAppliedToClusterTrafficPolicy 验证 ExternalTrafficPolicy=Cluster 时不会启用本地流量 DSR。
func TestDsrNotAppliedToClusterTrafficPolicy(t *testing.T) {
	proxier := NewFakeProxier(t, "testhost", netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Fatal("Failed to create proxier")
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "ClusterIP"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyCluster
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())
	}

	if svcInfo.localTrafficDSR {
		t.Errorf("Expected localTrafficDSR to be false for ExternalTrafficPolicy=Cluster, but got true")
	}
}

// TestClusterIPSvcWithITPLocal 测试 InternalTrafficPolicy=Local 的 ClusterIP 服务的以下场景：
//  1. 添加本地端点时，服务应继续使用本地端点和现有负载均衡器；
//     若当前没有负载均衡器，则应新建一个。
//  2. 再添加一个本地端点时，服务应删除现有负载均衡器并新建一个。
//  3. 添加远端端点时，服务应继续使用本地端点和现有负载均衡器，
//     因为这是 InternalTrafficPolicy=Local 服务。
//
// 流量策略为 Cluster 时不使用 DSR，随后验证 localTrafficDSR 保持为 false。
func TestClusterIPSvcWithITPLocal(t *testing.T) {
	proxier := NewFakeProxier(t, "testhost", netutils.ParseIPSloppy("10.0.0.1"), "L2Bridge", true)
	if proxier == nil {
		t.Fatal("Failed to create proxier")
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	// 使用 CNI 管理的本地端点需预置到 mock 中，否则代理无法查询到它。
	itpLocal := v1.ServiceInternalTrafficPolicyLocal

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = svcIP
			svc.Spec.InternalTrafficPolicy = &itpLocal // 将 InternalTrafficPolicy 设为 Local
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{epIpAddressLocal1}, // 本地端点 1
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	hcn := (proxier.hcn).(*fakehcn.HcnMock)
	// 本地端点由 CNI 创建，这里靠预填充 mock 来模拟这一前提。
	// 将该端点填充到缓存中，因为它是本地端点，而本地端点由 CNI 而非 kube-proxy 管理；
	// 在此填充即将其标记为本地端点。
	hcn.PopulateQueriedEndpoints(endpointLocal1, networkId, epIpAddressLocal1, macAddressLocal1, prefixLen)

	proxier.setInitialized(true)

	// 测试 1：添加本地端点时，服务应继续使用本地端点和现有负载均衡器；
	// 若当前没有负载均衡器，则应新建一个。
	proxier.syncProxyRules()

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	assert.True(t, ok, fmt.Sprintf("Failed to cast endpointInfo %q", svcPortName.String()))
	assert.NotEmpty(t, epInfo.hnsID, fmt.Sprintf("Expected HNS ID to be set for endpoint %s, but got empty value", epIpAddressRemote))

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, svcInfo.hnsID, loadbalancerGuid1, fmt.Sprintf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1))
	lb, err := proxier.hcn.GetLoadBalancerByID(loadbalancerGuid1)
	assert.Equal(t, nil, err, fmt.Sprintf("Failed to fetch loadbalancer: %s. Error: %v", loadbalancerGuid1, err))
	assert.NotNil(t, lb, "Loadbalancer object should not be nil")

	// 测试 2：再添加一个本地端点时，服务应删除现有负载均衡器并新建一个。

	// 重置初始化状态，使下一次同步按变更后的端点重建负载均衡器。
	proxier.setInitialized(false)

	proxier.OnEndpointSliceUpdate(
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressLocal1},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{epIpAddressLocal1},
				},
				{
					Addresses: []string{epIpAddressLocal2}, // 再增加一个本地端点
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}))

	// 修改内部状态需要持锁，避免与 syncProxyRules 并发访问产生数据竞争。
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	proxier.setInitialized(true)

	// 创建第二个本地端点
	hcn.PopulateQueriedEndpoints(endpointLocal2, networkId, epIpAddressLocal2, macAddressLocal2, prefixLen)
	// 重新执行 syncProxyRules，用新的本地端点创建新的负载均衡器
	proxier.syncProxyRules()
	svc = proxier.svcPortMap[svcPortName]
	svcInfo, ok = svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, svcInfo.hnsID, loadbalancerGuid2, fmt.Sprintf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid2))
	lb, err = proxier.hcn.GetLoadBalancerByID(loadbalancerGuid2)
	assert.Equal(t, nil, err, fmt.Sprintf("Failed to fetch loadbalancer: %s. Error: %v", loadbalancerGuid2, err))
	assert.NotNil(t, lb, "Loadbalancer object should not be nil")

	lb, _ = proxier.hcn.GetLoadBalancerByID(loadbalancerGuid1)
	assert.Nil(t, lb, fmt.Sprintf("Loadbalancer object should be nil: %s", loadbalancerGuid1))

	// 测试 3：添加远端端点时，服务应继续使用本地端点和现有负载均衡器，
	// 因为这是 InternalTrafficPolicy=Local 服务。

	// 再次重置初始化，使远端端点加入后的同步重新计算规则。
	proxier.setInitialized(false)

	proxier.OnEndpointSliceUpdate(
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{epIpAddressLocal1},
				},
				{
					Addresses: []string{epIpAddressLocal2},
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{epIpAddressLocal1},
				},
				{
					Addresses: []string{epIpAddressLocal2}, // 再增加一个本地端点
				},
				{
					Addresses: []string{epIpAddressRemote}, // 向 slice 中再增加一个远端端点
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}))

	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	proxier.setInitialized(true)

	proxier.syncProxyRules()
	svc = proxier.svcPortMap[svcPortName]
	svcInfo, ok = svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, svcInfo.hnsID, loadbalancerGuid2, fmt.Sprintf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid2))
	lb, err = proxier.hcn.GetLoadBalancerByID(loadbalancerGuid2)
	assert.Equal(t, nil, err, fmt.Sprintf("Failed to fetch loadbalancer: %s. Error: %v", loadbalancerGuid2, err))
	assert.NotNil(t, lb, "Loadbalancer object should not be nil")
}

// TestSharedRemoteEndpointDelete 验证两个服务共享远端端点时，删除其一后引用计数递减且端点保留。
func TestSharedRemoteEndpointDelete(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), "L2Bridge", true)
	if proxier == nil {
		t.Error()
	}

	svcIP1 := "10.20.30.41"
	svcPort1 := 80
	svcNodePort1 := 3001
	svcPortName1 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcIP2 := "10.20.30.42"
	svcPort2 := 80
	svcNodePort2 := 3002
	svcPortName2 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc2"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort1),
			}}
		}),
		makeTestService(svcPortName2.Namespace, svcPortName2.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP2
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName2.Port,
				Port:     int32(svcPort2),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort2),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)
	// 同一 HNS 端点被两个服务共享，引用计数应为 2。
	proxier.setInitialized(true)
	proxier.syncProxyRules()
	ep := proxier.endpointsMap[svcPortName1][0]
	epInfo, ok := ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName1.String())

	} else {
		if epInfo.hnsID != endpointGuid1 {
			t.Errorf("%v does not match %v", epInfo.hnsID, endpointGuid1)
		}
	}

	if *proxier.endPointsRefCount[endpointGuid1] != 2 {
		t.Errorf("RefCount not incremented. Current value: %v", *proxier.endPointsRefCount[endpointGuid1])
	}

	if *proxier.endPointsRefCount[endpointGuid1] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}

	// 删除一个服务后重新同步，验证引用计数递减而非立即删除共享端点。
	proxier.setInitialized(false)
	deleteServices(proxier,
		makeTestService(svcPortName2.Namespace, svcPortName2.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP2
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName2.Port,
				Port:     int32(svcPort2),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort2),
			}}
		}),
	)

	deleteEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	ep = proxier.endpointsMap[svcPortName1][0]
	epInfo, ok = ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName1.String())

	} else {
		if epInfo.hnsID != endpointGuid1 {
			t.Errorf("%v does not match %v", epInfo.hnsID, endpointGuid1)
		}
	}

	if *epInfo.refCount != 1 {
		t.Errorf("Incorrect Refcount. Current value: %v", *epInfo.refCount)
	}

	if *proxier.endPointsRefCount[endpointGuid1] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}
}

// TestSharedRemoteEndpointUpdate 验证服务与 EndpointSlice 更新后共享远端端点的引用计数保持一致。
func TestSharedRemoteEndpointUpdate(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), "L2Bridge", true)
	if proxier == nil {
		t.Error()
	}

	svcIP1 := "10.20.30.41"
	svcPort1 := 80
	svcNodePort1 := 3001
	svcPortName1 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcIP2 := "10.20.30.42"
	svcPort2 := 80
	svcNodePort2 := 3002
	svcPortName2 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc2"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort1),
			}}
		}),
		makeTestService(svcPortName2.Namespace, svcPortName2.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP2
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName2.Port,
				Port:     int32(svcPort2),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort2),
			}}
		}),
	)

	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()
	ep := proxier.endpointsMap[svcPortName1][0]
	epInfo, ok := ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName1.String())

	} else {
		if epInfo.hnsID != endpointGuid1 {
			t.Errorf("%v does not match %v", epInfo.hnsID, endpointGuid1)
		}
	}

	if *proxier.endPointsRefCount[endpointGuid1] != 2 {
		t.Errorf("RefCount not incremented. Current value: %v", *proxier.endPointsRefCount[endpointGuid1])
	}

	if *proxier.endPointsRefCount[endpointGuid1] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}

	// 更新服务前先重置初始化，使后续同步走增量变更路径。
	proxier.setInitialized(false)

	proxier.OnServiceUpdate(
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort1),
			}}
		}),
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(3003),
			}}
		}))

	proxier.OnEndpointSliceUpdate(
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			},
				{
					Name:     ptr.To("p443"),
					Port:     ptr.To[int32](443),
					Protocol: ptr.To(v1.ProtocolTCP),
				}}
		}))

	// 手动标记 EndpointSlice 已同步，跳过 informer 的同步门槛。
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	ep = proxier.endpointsMap[svcPortName1][0]
	epInfo, ok = ep.(*endpointInfo)

	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName1.String())

	} else {
		if epInfo.hnsID != endpointGuid1 {
			t.Errorf("%v does not match %v", epInfo.hnsID, endpointGuid1)
		}
	}

	if *epInfo.refCount != 2 {
		t.Errorf("Incorrect refcount. Current value: %v", *epInfo.refCount)
	}

	if *proxier.endPointsRefCount[endpointGuid1] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}
}

// TestCreateLoadBalancerWithoutDSR 验证未开启 DSR 时负载均衡器标志为 LoadBalancerFlagsNone。
func TestCreateLoadBalancerWithoutDSR(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, false)
	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	// 校验标志为 None，防止关闭 DSR 时误用 DSR 标志。
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	}

	if svcInfo.hnsID != loadbalancerGuid1 {
		t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
	}

	lb, err := proxier.hcn.GetLoadBalancerByID(svcInfo.hnsID)
	if err != nil {
		t.Errorf("Failed to fetch loadbalancer: %v", err)
	}

	if lb == nil {
		t.Errorf("Failed to fetch loadbalancer: %v", err)
	}

	if lb.Flags != hcn.LoadBalancerFlagsNone {
		t.Errorf("Incorrect loadbalancer flags. Current value: %v", lb.Flags)
	}
}

// TestCreateLoadBalancerWithDSR 验证开启 DSR 时负载均衡器标志为 LoadBalancerFlagsDSR。
func TestCreateLoadBalancerWithDSR(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	// 校验标志为 DSR，确保开启 DSR 的配置真正生效。
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	}

	if svcInfo.hnsID != loadbalancerGuid1 {
		t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
	}

	lb, err := proxier.hcn.GetLoadBalancerByID(svcInfo.hnsID)
	if err != nil {
		t.Errorf("Failed to fetch loadbalancer: %v", err)
	}

	if lb == nil {
		t.Errorf("Failed to fetch loadbalancer: %v", err)
	}

	if lb.Flags != hcn.LoadBalancerFlagsDSR {
		t.Errorf("Incorrect loadbalancer flags. Current value: %v", lb.Flags)
	}
}

// TestUpdateLoadBalancerWhenSupported 验证 HCN 支持修改时端点变更原地更新负载均衡器，ID 保持不变。
func TestUpdateLoadBalancerWhenSupported(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	// 显式开启 ModifyLoadbalancer，模拟支持原地更新的 HCN 版本。
	proxier.supportedFeatures.ModifyLoadbalancer = true

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		if svcInfo.hnsID != loadbalancerGuid1 {
			t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
		}
	}

	// 先同步建立基线，再更新端点以触发增量更新路径。
	proxier.setInitialized(false)

	proxier.OnEndpointSliceUpdate(
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epPaAddress},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}))

	// 持锁设置同步标志，避免与可能的并发同步发生数据竞争。
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	proxier.setInitialized(true)

	epObj, err := proxier.hcn.GetEndpointByID("EPID-3")
	if err != nil || epObj == nil {
		t.Errorf("Failed to fetch endpoint: EPID-3")
	}

	// 触发同步，使端点变更应用到 HNS。
	proxier.syncProxyRules()

	// 该端点应被删除，因为它已不在新的 EndpointSlice 中
	epObj, err = proxier.hcn.GetEndpointByID("EPID-3")
	if err == nil || epObj != nil {
		t.Errorf("Failed to fetch endpoint: EPID-3")
	}

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)

	epObj, err = proxier.hcn.GetEndpointByID("EPID-5")
	if err != nil || epObj == nil {
		t.Errorf("Failed to fetch endpoint: EPID-5")
	}

	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName.String())

	} else {
		if epInfo.hnsID != "EPID-5" {
			t.Errorf("%v does not match %v", epInfo.hnsID, "EPID-5")
		}
	}

	if *epInfo.refCount != 1 {
		t.Errorf("Incorrect refcount. Current value: %v", *epInfo.refCount)
	}

	if *proxier.endPointsRefCount["EPID-5"] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}

	svc = proxier.svcPortMap[svcPortName]
	svcInfo, ok = svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		// 更新后负载均衡器 ID 不应改变
		if svcInfo.hnsID != loadbalancerGuid1 {
			t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
		}
	}

}

// TestUpdateLoadBalancerWhenUnsupported 验证 HCN 不支持修改时会重建负载均衡器并分配新 ID。
func TestUpdateLoadBalancerWhenUnsupported(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	// 该值默认为 false，为提升测试可读性在此显式再次设为 false
	// 显式置为 false 以覆盖不支持修改负载均衡器的分支。
	proxier.supportedFeatures.ModifyLoadbalancer = false

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		if svcInfo.hnsID != loadbalancerGuid1 {
			t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
		}
	}

	proxier.setInitialized(false)

	proxier.OnEndpointSliceUpdate(
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epPaAddress},
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}))

	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	proxier.setInitialized(true)

	epObj, err := proxier.hcn.GetEndpointByID("EPID-3")
	if err != nil || epObj == nil {
		t.Errorf("Failed to fetch endpoint: EPID-3")
	}

	proxier.syncProxyRules()

	// 该端点应被删除，因为它已不在新的 EndpointSlice 中
	epObj, err = proxier.hcn.GetEndpointByID("EPID-3")
	if err == nil || epObj != nil {
		t.Errorf("Failed to fetch endpoint: EPID-3")
	}

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)

	epObj, err = proxier.hcn.GetEndpointByID("EPID-5")
	if err != nil || epObj == nil {
		t.Errorf("Failed to fetch endpoint: EPID-5")
	}

	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName.String())

	} else {
		if epInfo.hnsID != "EPID-5" {
			t.Errorf("%v does not match %v", epInfo.hnsID, "EPID-5")
		}
	}

	if *epInfo.refCount != 1 {
		t.Errorf("Incorrect refcount. Current value: %v", *epInfo.refCount)
	}

	if *proxier.endPointsRefCount["EPID-5"] != *epInfo.refCount {
		t.Errorf("Global refCount: %v does not match endpoint refCount: %v", *proxier.endPointsRefCount[endpointGuid1], *epInfo.refCount)
	}

	svc = proxier.svcPortMap[svcPortName]
	svcInfo, ok = svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		// 更新后负载均衡器 ID 应改变
		if svcInfo.hnsID != "LBID-3" {
			t.Errorf("%v does not match %v", svcInfo.hnsID, "LBID-3")
		}
	}

}

// TestCreateDsrLoadBalancer 验证 ExternalTrafficPolicy=Local 时创建 DSR 负载均衡器及健康检查 LB。
func TestCreateDsrLoadBalancer(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}
	lbIP := "11.21.31.41"

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: lbIP,
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	hcn := (proxier.hcn).(*fakehcn.HcnMock)
	// 预填充网关端点，供 DSR 规则引用。
	proxier.rootHnsEndpointName = endpointGw
	hcn.PopulateQueriedEndpoints(endpointLocal1, guid, epIpAddressRemote, macAddress, prefixLen)
	hcn.PopulateQueriedEndpoints(endpointGw, guid, epIpAddressGw, epMacAddressGw, prefixLen)
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		if svcInfo.hnsID != loadbalancerGuid1 {
			t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
		}
		if svcInfo.localTrafficDSR != true {
			t.Errorf("Failed to create DSR loadbalancer with local traffic policy")
		}
		// 断言 Ingress IP 对应的健康检查负载均衡器 ID 已建立。
		if len(svcInfo.loadBalancerIngressIPs) == 0 {
			t.Errorf("svcInfo does not have any loadBalancerIngressIPs, %+v", svcInfo)
		}
		if svcInfo.loadBalancerIngressIPs[0].healthCheckHnsID != "LBID-4" {
			t.Errorf("The Hns Loadbalancer HealthCheck Id %v does not match %v. ServicePortName %q", svcInfo.loadBalancerIngressIPs[0].healthCheckHnsID, loadbalancerGuid1, svcPortName.String())
		}
	}
}

// TestClusterIPLBInCreateDsrLoadBalancer 测试：当可用端点均为远端端点时，
// syncProxyRules 只创建 ClusterIP 负载均衡器，不会创建 NodePort、ExternalIP 或 IngressIP
// 负载均衡器。
// TestClusterIPLBInCreateDsrLoadBalancer 验证远端端点场景下只创建 ClusterIP 负载均衡器。
func TestClusterIPLBInCreateDsrLoadBalancer(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, false)

	if proxier == nil {
		t.Error()
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}
	lbIP := "11.21.31.41"

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
			svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
				IP: lbIP,
			}}
		}),
	)
	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{epIpAddressRemote},
				NodeName:  ptr.To("testhost2"), // 该 NodeName 与代理主机名不同，使该端点成为远端端点
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	// 端点全为远端时不应创建 NodePort/ExternalIP/IngressIP 负载均衡器。
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		// 检查 ClusterIP 负载均衡器已创建
		if svcInfo.hnsID != loadbalancerGuid1 {
			t.Errorf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1)
		}
		// 验证 NodePort 负载均衡器未创建
		if svcInfo.nodePorthnsID != "" {
			t.Errorf("NodePortHnsID %v is not empty.", svcInfo.nodePorthnsID)
		}
		// 验证 ExternalIP 负载均衡器未创建
		for _, externalIP := range svcInfo.externalIPs {
			if externalIP.hnsID != "" {
				t.Errorf("ExternalLBID %v is not empty.", externalIP.hnsID)
			}
		}
		// 验证 IngressIP 负载均衡器未创建
		for _, ingressIP := range svcInfo.loadBalancerIngressIPs {
			if ingressIP.hnsID != "" {
				t.Errorf("IngressLBID %v is not empty.", ingressIP.hnsID)
			}
		}
	}
}

// TestEndpointSliceWithInternalPortDifferentFromServicePort 验证端口映射使用容器端口作内部端口。
func TestEndpointSliceWithInternalPortDifferentFromServicePort(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	assert.NotNil(t, proxier, "Failed to create proxier")

	// 手动标记资源已同步，使 Add 事件中的变更进入缓存。
	proxier.servicesSynced = true
	proxier.endpointSlicesSynced = true

	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcSpec := v1.ServiceSpec{
		ClusterIP: "172.20.1.1",
		Selector:  map[string]string{"foo": "bar"},
		Ports: []v1.ServicePort{
			{Name: svcPortName.Port, Port: 80, TargetPort: intstr.FromInt32(80), Protocol: v1.ProtocolTCP}, // 模拟 TargetPort 与服务端口一致（80）
		},
	}

	proxier.OnServiceAdd(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcPortName.Name, Namespace: svcPortName.Namespace},
		Spec:       svcSpec,
	})

	// 添加初始 EndpointSlice
	endpointSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-1", svcPortName.Name),
			Namespace: svcPortName.Namespace,
			Labels:    map[string]string{discovery.LabelServiceName: svcPortName.Name},
		},
		Ports: []discovery.EndpointPort{{
			Name:     &svcPortName.Port,
			Port:     ptr.To[int32](8080), // 使用与服务端口 80 不同的容器端口 8080
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses:  []string{"192.168.2.3"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To("testhost2"),
		}},
	}

	// 添加切片后同步规则，验证端口映射由 EndpointSlice 端口决定。
	proxier.OnEndpointSliceAdd(endpointSlice)
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, svcInfo.hnsID, loadbalancerGuid1, "The Hns Loadbalancer Id %v does not match %v. ServicePortName %q", svcInfo.hnsID, loadbalancerGuid1, svcPortName.String())

	lb, err := proxier.hcn.GetLoadBalancerByID(svcInfo.hnsID)
	assert.Equal(t, nil, err, "Failed to fetch loadbalancer: %v", err)
	assert.NotEqual(t, nil, lb, "Loadbalancer object should not be nil")
	assert.Equal(t, len(lb.PortMappings), 1, "PortMappings should have one and only one entry")
	assert.Equal(t, lb.PortMappings[0].InternalPort, uint16(8080), "InternalPort should be 8080")
	assert.Equal(t, lb.PortMappings[0].ExternalPort, uint16(80), "ExternalPort should be 80")

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	assert.True(t, ok, "Failed to cast endpointInfo %q", svcPortName.String())
	assert.Equal(t, epInfo.hnsID, "EPID-3", "Hns EndpointId %v does not match %v. ServicePortName %q", epInfo.hnsID, endpointGuid1, svcPortName.String())
}

// TestEndpointSlice 验证 Add 事件后同步规则能创建正确的负载均衡器与端点。
func TestEndpointSlice(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	// 直接设置同步标志，避免依赖完整 informer 启动流程。
	proxier.servicesSynced = true
	proxier.endpointSlicesSynced = true

	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	proxier.OnServiceAdd(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcPortName.Name, Namespace: svcPortName.Namespace},
		Spec: v1.ServiceSpec{
			ClusterIP: "172.20.1.1",
			Selector:  map[string]string{"foo": "bar"},
			Ports:     []v1.ServicePort{{Name: svcPortName.Port, TargetPort: intstr.FromInt32(80), Protocol: v1.ProtocolTCP}},
		},
	})

	// 添加初始 EndpointSlice
	endpointSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-1", svcPortName.Name),
			Namespace: svcPortName.Namespace,
			Labels:    map[string]string{discovery.LabelServiceName: svcPortName.Name},
		},
		Ports: []discovery.EndpointPort{{
			Name:     &svcPortName.Port,
			Port:     ptr.To[int32](80),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses:  []string{"192.168.2.3"},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			NodeName:   ptr.To("testhost2"),
		}},
	}

	proxier.OnEndpointSliceAdd(endpointSlice)
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())

	} else {
		if svcInfo.hnsID != loadbalancerGuid1 {
			t.Errorf("The Hns Loadbalancer Id %v does not match %v. ServicePortName %q", svcInfo.hnsID, loadbalancerGuid1, svcPortName.String())
		}
	}

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	if !ok {
		t.Errorf("Failed to cast endpointInfo %q", svcPortName.String())

	} else {
		if epInfo.hnsID != "EPID-3" {
			t.Errorf("Hns EndpointId %v does not match %v. ServicePortName %q", epInfo.hnsID, endpointGuid1, svcPortName.String())
		}
	}
}

// TestNoopEndpointSlice 验证空 EndpointSlice 事件不会让空 Proxier panic。
func TestNoopEndpointSlice(t *testing.T) {
	// 仅初始化变更跟踪器即可，无需完整 Proxier。
	p := Proxier{}
	p.endpointsChanges = proxy.NewEndpointsChangeTracker(v1.IPv4Protocol, "", nil, nil)
	p.OnEndpointSliceAdd(&discovery.EndpointSlice{})
	p.OnEndpointSliceUpdate(&discovery.EndpointSlice{}, &discovery.EndpointSlice{})
	p.OnEndpointSliceDelete(&discovery.EndpointSlice{})
	p.OnEndpointSlicesSynced()
}

// TestFindRemoteSubnetProviderAddress 验证按 provider address 或子网 IP 均可查到 provider address。
func TestFindRemoteSubnetProviderAddress(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error()
	}

	// 使用 mock 中预置的远端子网，避免依赖真实网络配置。
	networkInfo, _ := proxier.hns.getNetworkByName(testNetwork)
	pa := networkInfo.findRemoteSubnetProviderAddress(providerAddress)

	if pa != providerAddress {
		t.Errorf("%v does not match %v", pa, providerAddress)
	}

	pa = networkInfo.findRemoteSubnetProviderAddress(epIpAddressRemote)

	if pa != providerAddress {
		t.Errorf("%v does not match %v", pa, providerAddress)
	}

	pa = networkInfo.findRemoteSubnetProviderAddress(serviceVip)

	if len(pa) != 0 {
		t.Errorf("Provider address is not empty as expected")
	}
}

// TestWinDSRWithOverlayEnabled 验证 overlay 下 WinDSR 门控开启后本地流量 DSR 生效。
func TestWinDSRWithOverlayEnabled(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, true)
	if proxier == nil {
		t.Error("Failed to create proxier")
	}

	svcIP := "10.20.30.41"
	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = "ClusterIP"
			svc.Spec.ClusterIP = svcIP
			svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort),
			}}
		}),
	)

	proxier.setInitialized(true)
	proxier.syncProxyRules()

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	if !ok {
		t.Errorf("Failed to cast serviceInfo %q", svcPortName.String())
	}

	if !svcInfo.localTrafficDSR {
		t.Errorf("Expected localTrafficDSR to be enabled but got false")
	}
}

// testDualStackService 为双栈服务分别构造 IPv4/IPv6 Proxier 并共享 HCN mock，
// 按流量策略与 IP 家族策略验证各负载均衡器的 ID、标志与端点数量。
func testDualStackService(t *testing.T, enableDsr bool, dualStackFamilyPolicy v1.IPFamilyPolicy, trafficPolicy v1.ServiceExternalTrafficPolicy) {
	baseFlag := hcn.LoadBalancerFlagsNone
	if trafficPolicy == v1.ServiceExternalTrafficPolicyLocal {
		baseFlag = hcn.LoadBalancerFlagsDSR
	}

	// 分别构造 v4/v6 Proxier，模拟双栈服务在两个地址族上的同步。
	v4Proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_OVERLAY, enableDsr)
	if v4Proxier == nil {
		t.Error()
	}
	v6Proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10::1"), NETWORK_TYPE_OVERLAY, enableDsr, v1.IPv6Protocol)
	if v6Proxier == nil {
		t.Error()
	}
	v6Proxier.hcn = v4Proxier.hcn // 共享同一个 HCN mock 以模拟双栈行为

	svcPort := 80
	svcNodePort := 3001
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	v4ClusterIP, v6ClusterIP := "10.20.30.41", "fd00::21"
	v4LbIP, v6LbIP := "11.21.31.41", "fd00::1"

	testService := makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeLoadBalancer
		svc.Spec.ClusterIP = v4ClusterIP
		svc.Spec.ClusterIPs = []string{v4ClusterIP, v6ClusterIP}
		svc.Spec.IPFamilyPolicy = &dualStackFamilyPolicy
		svc.Spec.IPFamilies = []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}
		svc.Spec.ExternalTrafficPolicy = trafficPolicy
		svc.Spec.Ports = []v1.ServicePort{{
			Name:     svcPortName.Port,
			Port:     int32(svcPort),
			Protocol: v1.ProtocolTCP,
			NodePort: int32(svcNodePort),
		}}
		svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{
			{IP: v4LbIP},
			{IP: v6LbIP},
		}
	})

	v4Endpoints := []discovery.Endpoint{{
		Addresses: []string{epIpAddressLocal},
		NodeName:  ptr.To(testNodeName),
	}}

	v6Endpoints := []discovery.Endpoint{{
		Addresses: []string{epIpAddressLocalv6},
		NodeName:  ptr.To(testNodeName),
	}}

	if trafficPolicy == v1.ServiceExternalTrafficPolicyCluster {
		v4Endpoints = append(v4Endpoints, discovery.Endpoint{
			Addresses: []string{epIpAddressRemote},
		})
		v6Endpoints = append(v6Endpoints, discovery.Endpoint{
			Addresses: []string{epIpAddressRemotev6},
		})
	}

	testEpSliceV4 := makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = v4Endpoints
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(svcPortName.Port),
			Port:     ptr.To(int32(svcPort)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	})

	testEpSliceV6 := makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv6
		eps.Endpoints = v6Endpoints
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(svcPortName.Port),
			Port:     ptr.To(int32(svcPort)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	})

	makeServiceMap(v4Proxier, testService)
	makeServiceMap(v6Proxier, testService)

	populateEndpointSlices(v4Proxier, testEpSliceV4)
	populateEndpointSlices(v6Proxier, testEpSliceV6)

	// 共享同一 mock 才能验证两个 Proxier 对同一 HNS 状态的操作顺序。
	hcnMock := (v4Proxier.hcn).(*fakehcn.HcnMock)
	v4Proxier.rootHnsEndpointName = endpointGw
	v6Proxier.rootHnsEndpointName = endpointGw
	hcnMock.PopulateQueriedEndpoints(endpointLocal1, guid, epIpAddressLocal, macAddress, prefixLen)
	hcnMock.PopulateQueriedEndpoints(endpointLocal1, guid, epIpAddressLocalv6, macAddress, prefixLen)
	hcnMock.PopulateQueriedEndpoints(endpointGw, guid, epIpAddressGw, epMacAddressGw, prefixLen)
	hcnMock.PopulateQueriedEndpoints(endpointGw, guid, epIpAddressGwv6, epMacAddressGw, prefixLen)
	v4Proxier.setInitialized(true)
	// 先同步 v4 再同步 v6，模拟真实双栈同步顺序。
	v4Proxier.syncProxyRules()

	v4Svc := v4Proxier.svcPortMap[svcPortName]
	v4SvcInfo, ok := v4Svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, loadbalancerGuids[1], v4SvcInfo.hnsID, "The Hns Loadbalancer Id %v does not match %v. ServicePortName %q", v4SvcInfo.hnsID, loadbalancerGuids[1], svcPortName.String())
	assert.Equal(t, v4LbIP, v4SvcInfo.loadBalancerIngressIPs[0].ip, "LoadBalancer Ingress IP 0 should be %s", v4LbIP)
	assert.Equal(t, loadbalancerGuids[2], v4SvcInfo.nodePorthnsID, "The Hns NodePort Loadbalancer Id %v does not match %v. ServicePortName %q", v4SvcInfo.nodePorthnsID, loadbalancerGuids[2], svcPortName.String())
	assert.Equal(t, 1, len(v4SvcInfo.loadBalancerIngressIPs), "Expected 1 LoadBalancer Ingress IP, found %d", len(v4SvcInfo.loadBalancerIngressIPs))
	assert.Equal(t, loadbalancerGuids[3], v4SvcInfo.loadBalancerIngressIPs[0].hnsID, "The Hns Loadbalancer Ingress Id %v does not match %v. ServicePortName %q", v4SvcInfo.loadBalancerIngressIPs[0].hnsID, loadbalancerGuids[3], svcPortName.String())
	assert.Equal(t, loadbalancerGuids[4], v4SvcInfo.loadBalancerIngressIPs[0].healthCheckHnsID, "The Hns Loadbalancer HealthCheck Id %v does not match %v. ServicePortName %q", v4SvcInfo.loadBalancerIngressIPs[0].healthCheckHnsID, loadbalancerGuids[4], svcPortName.String())

	// 验证 ClusterIP 负载均衡器
	clusterIPLb, err := v4Proxier.hcn.GetLoadBalancerByID(v4SvcInfo.hnsID)
	assert.Equal(t, nil, err, "Failed to fetch loadbalancer: %v", err)
	assert.NotEqual(t, nil, clusterIPLb, "Loadbalancer object should not be nil")
	assert.Equal(t, baseFlag, clusterIPLb.Flags, "Loadbalancer Flags should be %v for IPv4 stack", baseFlag)

	// 验证 NodePort 负载均衡器
	nodePortLb, err := v4Proxier.hcn.GetLoadBalancerByID(v4SvcInfo.nodePorthnsID)
	assert.Equal(t, nil, err, "Failed to fetch NodePort loadbalancer: %v", err)
	assert.NotEqual(t, nil, nodePortLb, "NodePort Loadbalancer object should not be nil")
	assert.Equal(t, baseFlag, nodePortLb.Flags, "NodePort Loadbalancer Flags should be %v for IPv4 stack", baseFlag)

	// 验证 IPv4 IngressIP 负载均衡器
	ingressLb, err := v4Proxier.hcn.GetLoadBalancerByID(v4SvcInfo.loadBalancerIngressIPs[0].hnsID)
	assert.Equal(t, nil, err, "Failed to fetch IngressIP loadbalancer: %v", err)
	assert.NotEqual(t, nil, ingressLb, "IngressIP Loadbalancer object should not be nil")
	assert.Equal(t, baseFlag, ingressLb.Flags, "IngressIP Loadbalancer Flags should be %v for IPv4 stack", baseFlag)

	if trafficPolicy == v1.ServiceExternalTrafficPolicyLocal {
		assert.Equal(t, 1, len(clusterIPLb.HostComputeEndpoints), "Expected 1 endpoint for local traffic policy, found %d", len(clusterIPLb.HostComputeEndpoints))
		assert.Equal(t, 1, len(nodePortLb.HostComputeEndpoints), "Expected 1 endpoint for local traffic policy, found %d", len(nodePortLb.HostComputeEndpoints))
		assert.Equal(t, 1, len(ingressLb.HostComputeEndpoints), "Expected 1 endpoint for local traffic policy, found %d", len(ingressLb.HostComputeEndpoints))
	} else {
		assert.Equal(t, 2, len(clusterIPLb.HostComputeEndpoints), "Expected 2 endpoints for cluster traffic policy, found %d", len(clusterIPLb.HostComputeEndpoints))
		assert.Equal(t, 2, len(nodePortLb.HostComputeEndpoints), "Expected 2 endpoints for cluster traffic policy, found %d", len(nodePortLb.HostComputeEndpoints))
		assert.Equal(t, 2, len(ingressLb.HostComputeEndpoints), "Expected 2 endpoints for cluster traffic policy, found %d", len(ingressLb.HostComputeEndpoints))
	}

	// 开始 IPv6 测试。
	v6Proxier.setInitialized(true)
	v6Proxier.syncProxyRules()

	v6Svc := v6Proxier.svcPortMap[svcPortName]
	v6SvcInfo, ok := v6Svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, loadbalancerGuids[5], v6SvcInfo.hnsID, "The Hns Loadbalancer Id %v does not match %v. ServicePortName %q", v6SvcInfo.hnsID, loadbalancerGuids[5], svcPortName.String())
	assert.Equal(t, loadbalancerGuids[6], v6SvcInfo.nodePorthnsID, "The Hns NodePort Loadbalancer Id %v does not match %v. ServicePortName %q", v6SvcInfo.nodePorthnsID, loadbalancerGuids[6], svcPortName.String())
	assert.Equal(t, 1, len(v6SvcInfo.loadBalancerIngressIPs), "Expected 1 LoadBalancer Ingress IP, found %d", len(v6SvcInfo.loadBalancerIngressIPs))
	assert.Equal(t, v6LbIP, v6SvcInfo.loadBalancerIngressIPs[0].ip, "LoadBalancer Ingress IP 0 should be %s", v6LbIP)
	assert.Equal(t, loadbalancerGuids[7], v6SvcInfo.loadBalancerIngressIPs[0].hnsID, "The Hns Loadbalancer Ingress Id %v does not match %v for IPv6 LB. ServicePortName %q", v6SvcInfo.loadBalancerIngressIPs[0].hnsID, loadbalancerGuids[7], svcPortName.String())
	assert.Equal(t, loadbalancerGuids[8], v6SvcInfo.loadBalancerIngressIPs[0].healthCheckHnsID, "The Hns Loadbalancer HealthCheck Id %v does not match %v. ServicePortName %q", v6SvcInfo.loadBalancerIngressIPs[0].healthCheckHnsID, loadbalancerGuids[8], svcPortName.String())

	// 验证 ClusterIP 负载均衡器
	clusterIPLb, err = v6Proxier.hcn.GetLoadBalancerByID(v6SvcInfo.hnsID)
	assert.Equal(t, nil, err, "Failed to fetch loadbalancer: %v", err)
	assert.NotEqual(t, nil, clusterIPLb, "Loadbalancer object should not be nil")
	assert.Equal(t, baseFlag|hcn.LoadBalancerFlagsIPv6, clusterIPLb.Flags, "Loadbalancer Flags should be %v for IPv6 stack", baseFlag|hcn.LoadBalancerFlagsIPv6)

	// 验证 IPv6 IngressIP 负载均衡器
	ingressLb, err = v6Proxier.hcn.GetLoadBalancerByID(v6SvcInfo.loadBalancerIngressIPs[0].hnsID)
	assert.Equal(t, nil, err, "Failed to fetch IngressIP loadbalancer: %v", err)
	assert.NotEqual(t, nil, ingressLb, "IngressIP Loadbalancer object should not be nil")
	assert.Equal(t, baseFlag|hcn.LoadBalancerFlagsIPv6, ingressLb.Flags, "IngressIP Loadbalancer Flags should be %v for IPv6 stack", baseFlag|hcn.LoadBalancerFlagsIPv6)

	// 汇总 HNS 端点数量，验证不同流量策略下创建的端点总数。
	eps, _ := hcnMock.ListEndpoints()
	if trafficPolicy == v1.ServiceExternalTrafficPolicyLocal {
		assert.Equal(t, 10, len(eps), "Expected 10 endpoints for local traffic policy, found %d", len(eps))

		assert.Equal(t, 1, len(clusterIPLb.HostComputeEndpoints), "Expected 1 endpoint for local traffic policy, found %d", len(clusterIPLb.HostComputeEndpoints))
		assert.Equal(t, 1, len(nodePortLb.HostComputeEndpoints), "Expected 1 endpoint for local traffic policy, found %d", len(nodePortLb.HostComputeEndpoints))
		assert.Equal(t, 1, len(ingressLb.HostComputeEndpoints), "Expected 1 endpoint for local traffic policy, found %d", len(ingressLb.HostComputeEndpoints))
	} else {
		assert.Equal(t, 14, len(eps), "Expected 14 endpoints for cluster traffic policy, found %d", len(eps))

		assert.Equal(t, 2, len(clusterIPLb.HostComputeEndpoints), "Expected 2 endpoints for cluster traffic policy, found %d", len(clusterIPLb.HostComputeEndpoints))
		assert.Equal(t, 2, len(nodePortLb.HostComputeEndpoints), "Expected 2 endpoints for cluster traffic policy, found %d", len(nodePortLb.HostComputeEndpoints))
		assert.Equal(t, 2, len(ingressLb.HostComputeEndpoints), "Expected 2 endpoints for cluster traffic policy, found %d", len(ingressLb.HostComputeEndpoints))
	}
}

// TestETPLocalIngressIPServiceWithPreferDualStack 验证 PreferDualStack 且 Local 流量策略的双栈服务。
func TestETPLocalIngressIPServiceWithPreferDualStack(t *testing.T) {
	testDualStackService(t, true, v1.IPFamilyPolicyPreferDualStack, v1.ServiceExternalTrafficPolicyLocal)
}

// TestETPLocalIngressIPServiceWithRequireDualStack 验证 RequireDualStack 且 Local 流量策略的双栈服务。
func TestETPLocalIngressIPServiceWithRequireDualStack(t *testing.T) {
	testDualStackService(t, true, v1.IPFamilyPolicyRequireDualStack, v1.ServiceExternalTrafficPolicyLocal)
}

// TestETPClusterIngressIPServiceWithPreferDualStack 验证 PreferDualStack 且 Cluster 流量策略的双栈服务。
func TestETPClusterIngressIPServiceWithPreferDualStack(t *testing.T) {
	testDualStackService(t, false, v1.IPFamilyPolicyPreferDualStack, v1.ServiceExternalTrafficPolicyCluster)
}

// TestETPClusterIngressIPServiceWithRequireDualStack 验证 RequireDualStack 且 Cluster 流量策略的双栈服务。
func TestETPClusterIngressIPServiceWithRequireDualStack(t *testing.T) {
	testDualStackService(t, false, v1.IPFamilyPolicyRequireDualStack, v1.ServiceExternalTrafficPolicyCluster)
}

// makeNSN 将命名空间和名称组合成 NamespacedName，简化测试数据构造。
func makeNSN(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// makeServiceMap 批量添加服务并标记已同步，避免依赖 informer 回调。
func makeServiceMap(proxier *Proxier, allServices ...*v1.Service) {
	for i := range allServices {
		proxier.OnServiceAdd(allServices[i])
	}

	proxier.mu.Lock()
	defer proxier.mu.Unlock()
	proxier.servicesSynced = true
}

// deleteServices 批量删除服务并标记已同步，模拟 informer 删除事件后的状态。
func deleteServices(proxier *Proxier, allServices ...*v1.Service) {
	for i := range allServices {
		proxier.OnServiceDelete(allServices[i])
	}

	proxier.mu.Lock()
	defer proxier.mu.Unlock()
	proxier.servicesSynced = true
}

// makeTestService 构造带指定命名空间、名称的服务，其余字段由回调按需填充。
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

// deleteEndpointSlices 批量删除 EndpointSlice 并标记已同步，模拟删除事件。
func deleteEndpointSlices(proxier *Proxier, allEndpointSlices ...*discovery.EndpointSlice) {
	for i := range allEndpointSlices {
		proxier.OnEndpointSliceDelete(allEndpointSlices[i])
	}

	proxier.mu.Lock()
	defer proxier.mu.Unlock()
	proxier.endpointSlicesSynced = true
}

// populateEndpointSlices 批量添加 EndpointSlice 并标记已同步，模拟新增事件。
func populateEndpointSlices(proxier *Proxier, allEndpointSlices ...*discovery.EndpointSlice) {
	for i := range allEndpointSlices {
		proxier.OnEndpointSliceAdd(allEndpointSlices[i])
	}

	proxier.mu.Lock()
	defer proxier.mu.Unlock()
	proxier.endpointSlicesSynced = true
}

// makeTestEndpointSlice 构造带服务标签与序号的 EndpointSlice，其余字段由回调填充。
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

// testHostMacProvider 为测试提供固定的主机 MAC 地址，避免读取真实主机接口。
type testHostMacProvider struct {
	macAddress string
}

// GetHostMac 返回预设 MAC 地址；nodeIP 参数在此固定实现中被忽略。
func (r *testHostMacProvider) GetHostMac(nodeIP net.IP) string {
	return r.macAddress
}

// TestRemoteAndLocalEndpointsSameIP 演示一个引用计数问题：
// 两个服务共享同一 IP 地址的端点，其中一个服务
// 将其视为本地端点（NodeName 与代理主机名一致），另一个
// 视为远端端点（NodeName 不一致）。远端代理端点
// 会解析到本地 HNS 端点，导致其 refCount 从未通过
// 共享的 endPointsRefCount map 递增。
// 共享 IP 但本地/远端判定不同，会导致远端端点的引用计数遗漏。
func TestRemoteAndLocalEndpointsSameIP(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_L2BRIDGE, false)
	if proxier == nil {
		t.Error("Failed to create proxier")
	}

	sharedEPIP := epIpAddressLocal1 // "192.168.4.4" —— 两个服务共用同一 IP

	svcIP1 := "10.20.30.41"
	svcPort1 := 80
	svcPortName1 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcIP2 := "10.20.30.42"
	svcPort2 := 80
	svcPortName2 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc2"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		// svc1 将该端点作为本地端点使用
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
			}}
		}),
		// svc2 将该端点作为远端端点使用
		makeTestService(svcPortName2.Namespace, svcPortName2.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = svcIP2
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName2.Port,
				Port:     int32(svcPort2),
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(proxier,
		// svc1 的端点：本地（NodeName = "testhost" 与代理主机名一致）
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{sharedEPIP},
				NodeName:  ptr.To(testNodeName),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		// svc2 的端点：远端（NodeName = "testhost2" 与代理主机名不一致）
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{sharedEPIP},
				NodeName:  ptr.To("testhost2"),
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	// 预填充 sharedEPIP 处的本地 HNS 端点（模拟 CNI 创建该端点的行为）
	hcnMock := (proxier.hcn).(*fakehcn.HcnMock)
	hcnMock.PopulateQueriedEndpoints(endpointLocal1, networkId, sharedEPIP, macAddressLocal1, prefixLen)

	proxier.setInitialized(true)
	// 预填充后同步，确保本地与远端端点解析到同一 HNS 端点。
	proxier.syncProxyRules()

	// 查找每个服务的端点
	var localEp, remoteEp *endpointInfo
	for _, ep := range proxier.endpointsMap[svcPortName1] {
		if epI, ok := ep.(*endpointInfo); ok && epI.ip == sharedEPIP {
			localEp = epI
		}
	}
	for _, ep := range proxier.endpointsMap[svcPortName2] {
		if epI, ok := ep.(*endpointInfo); ok && epI.ip == sharedEPIP {
			remoteEp = epI
		}
	}

	assert.NotNil(t, localEp, "Expected to find local endpoint for svc1")
	assert.NotNil(t, remoteEp, "Expected to find remote endpoint for svc2")

	// 两者都应解析到同一个本地 HNS 端点
	assert.Equal(t, endpointLocal1, localEp.hnsID,
		"Local ep should have the pre-populated HNS endpoint ID")
	assert.Equal(t, endpointLocal1, remoteEp.hnsID,
		"Remote ep should resolve to the same local HNS endpoint ID")

	// 从代理层视角验证端点的本地/远端属性
	assert.True(t, localEp.IsLocal(), "svc1's endpoint should be local")
	assert.False(t, remoteEp.IsLocal(), "svc2's endpoint should be remote")

	// 远端端点的 refCount 从未通过 endPointsRefCount 递增，
	// 因为解析到的 HNS 端点是本地端点（newHnsEndpoint.IsLocal()=true），
	// 代码走了 hnsLocalEndpoints 分支，而不是递增
	// 共享 refCount。该远端端点保留其私有 refCount（值为 0）。
	// 断言远端端点保留私有 refCount 0，暴露引用计数遗漏。
	assert.NotNil(t, remoteEp.refCount, "Remote ep refCount pointer should not be nil")
	assert.Equal(t, uint16(0), *remoteEp.refCount,
		"Remote ep refCount should be 0 — it was never incremented because "+
			"the HNS endpoint is local, exposing a refCount tracking gap")

	// 共享的 endPointsRefCount map 中不应有该 HNS 端点的条目
	// （若因其他路径存在条目，其值也应为 0），
	// 因为只有远端 HNS 端点才纳入 refCount 统计。
	if refCountPtr, exists := proxier.endPointsRefCount[endpointLocal1]; exists {
		assert.Equal(t, uint16(0), *refCountPtr,
			"Shared refCount for the local HNS endpoint should be 0")
	}
	// 若该条目完全不存在，同样可以证实此问题：
	// 远端代理端点没有共享 refCount 记录。
}

// TestRemoteEndpointDeleteDoesNotAffectLocalEndpointWithSameIP 验证：
// 远端端点因端点映射变化被删除时，相同 IP 地址的本地端点不会被误删。
// 场景涉及两个服务共用的远端端点与第三个服务的本地端点 IP 重叠。
//
// 流程：
//  1. 创建本地端点（IP-A）和远端端点（IP-B）。
//  2. 服务 svc1、svc2 同时使用 IP-A 和 IP-B，远端端点 refCount = 2。
//  3. 新增服务 svc3，其本地端点位于 IP-B（与远端端点 IP 相同）；
//     远端 refCount 保持 2，本地端点 refCount 为 0（本地端点不参与引用计数）。
//  4. 移除 svc1、svc2 的端点，IP-B 处的远端端点应被清理（refCount 降为 0），
//     而 svc3 的本地端点必须保留。
//
// 这验证了按 HNS ID 清理远端端点时不会误删同 IP 的本地端点。
// 远端端点被清理时，不能误删同 IP 的本地端点（两者由 CNI/HNS 分别管理）。
func TestRemoteEndpointDeleteDoesNotAffectLocalEndpointWithSameIP(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_L2BRIDGE, false)
	if proxier == nil {
		t.Fatal("Failed to create proxier")
	}

	localIP := epIpAddressLocal1  // "192.168.4.4"
	remoteIP := epIpAddressRemote // "192.168.2.3"

	svcIP1 := "10.20.30.41"
	svcPort1 := 80
	svcNodePort1 := 3001
	svcPortName1 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcIP2 := "10.20.30.42"
	svcPort2 := 80
	svcNodePort2 := 3002
	svcPortName2 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc2"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcIP3 := "10.20.30.43"
	svcPort3 := 80
	svcPortName3 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc3"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	// 步骤 1+2：创建两个服务（svc1、svc2），各含一个本地端点（IP-A）
	// 和一个远端端点（IP-B）。
	makeServiceMap(proxier,
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort1),
			}}
		}),
		makeTestService(svcPortName2.Namespace, svcPortName2.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP2
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName2.Port,
				Port:     int32(svcPort2),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort2),
			}}
		}),
	)

	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{localIP},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{remoteIP},
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{localIP},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{remoteIP},
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	// 预填充本地 HNS 端点（模拟 CNI 创建该端点）。
	hcnMock := (proxier.hcn).(*fakehcn.HcnMock)
	hcnMock.PopulateQueriedEndpoints(endpointLocal1, networkId, localIP, macAddressLocal1, prefixLen)

	// 首次同步建立引用计数基线。
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	// 查找 svc1 的远端端点并断言 refCount == 2。
	var remoteEpInfo *endpointInfo
	var remoteEpHnsID string
	for _, ep := range proxier.endpointsMap[svcPortName1] {
		epI, ok := ep.(*endpointInfo)
		if ok && epI.ip == remoteIP {
			remoteEpInfo = epI
			remoteEpHnsID = epI.hnsID
		}
	}
	assert.NotNil(t, remoteEpInfo, "Expected to find remote endpoint for svc1")
	assert.NotEmpty(t, remoteEpHnsID, "Remote endpoint should have an HNS ID")
	assert.Equal(t, uint16(2), *remoteEpInfo.refCount,
		"Remote endpoint refCount should be 2 after two services reference it")
	assert.Equal(t, *proxier.endPointsRefCount[remoteEpHnsID], *remoteEpInfo.refCount,
		"Global and endpoint refCounts should match")

	// 步骤 3：新增 svc3，其本地端点位于 IP-B（与远端端点 IP 相同）。
	proxier.setInitialized(false)

	proxier.OnServiceAdd(
		makeTestService(svcPortName3.Namespace, svcPortName3.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.ClusterIP = svcIP3
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName3.Port,
				Port:     int32(svcPort3),
				Protocol: v1.ProtocolTCP,
			}}
		}))
	proxier.mu.Lock()
	proxier.servicesSynced = true
	proxier.mu.Unlock()

	proxier.OnEndpointSliceAdd(
		makeTestEndpointSlice(svcPortName3.Namespace, svcPortName3.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{{
				Addresses: []string{remoteIP},
				NodeName:  ptr.To(testNodeName), // NodeName 匹配，因此是本地端点
			}}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName3.Port),
				Port:     ptr.To(int32(svcPort3)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}))
	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	// 预填充 IP-B 处新的本地 HNS 端点。
	hcnMock.PopulateQueriedEndpoints(endpointLocal2, networkId, remoteIP, macAddressLocal2, prefixLen)

	// 重新同步，验证新增本地端点不影响共享远端端点的引用计数。
	proxier.setInitialized(true)
	proxier.syncProxyRules()

	// 远端端点 refCount 仍应为 2（svc1 和 svc2 仍引用它）。
	remoteEpInfo = nil
	for _, ep := range proxier.endpointsMap[svcPortName1] {
		epI, ok := ep.(*endpointInfo)
		if ok && epI.ip == remoteIP {
			remoteEpInfo = epI
		}
	}
	assert.NotNil(t, remoteEpInfo, "Expected remote endpoint still present for svc1")
	assert.Equal(t, uint16(2), *remoteEpInfo.refCount,
		"Remote endpoint refCount should still be 2 after adding local endpoint with same IP")

	// 查找 svc3 在 IP-B 处的本地端点；其 refCount 应为 0
	// （本地端点不参与引用计数）。
	var localEpAtRemoteIP *endpointInfo
	for _, ep := range proxier.endpointsMap[svcPortName3] {
		epI, ok := ep.(*endpointInfo)
		if ok && epI.ip == remoteIP {
			localEpAtRemoteIP = epI
		}
	}
	assert.NotNil(t, localEpAtRemoteIP, "Expected to find local endpoint for svc3 at IP-B")
	assert.True(t, localEpAtRemoteIP.IsLocal(), "svc3's endpoint should be local")
	assert.NotNil(t, localEpAtRemoteIP.refCount, "Local endpoint refCount pointer should not be nil")
	assert.Equal(t, uint16(0), *localEpAtRemoteIP.refCount,
		"Local endpoint refCount should be 0 (locals are not ref-counted)")

	// 步骤 4：移除 svc1 和 svc2 的 EndpointSlice（IP-A 和 IP-B 随之消失）。
	// 这应使 IP-B 处的远端端点被清理，
	// 但 svc3 在 IP-B 处的本地端点不得被删除。
	proxier.setInitialized(false)

	deleteEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{localIP},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{remoteIP},
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{localIP},
					NodeName:  ptr.To(testNodeName),
				},
				{
					Addresses: []string{remoteIP},
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	proxier.setInitialized(true)
	// 同步后验证远端端点已清理，而本地端点仍存在。
	proxier.syncProxyRules()

	// 此时 svc1 和 svc2 应没有任何端点。
	assert.Empty(t, proxier.endpointsMap[svcPortName1],
		"svc1 should have no endpoints after deletion")
	assert.Empty(t, proxier.endpointsMap[svcPortName2],
		"svc2 should have no endpoints after deletion")

	// svc3 在 IP-B 处的本地端点仍应存在且可用。
	var svc3Ep *endpointInfo
	for _, ep := range proxier.endpointsMap[svcPortName3] {
		epI, ok := ep.(*endpointInfo)
		if ok && epI.ip == remoteIP {
			svc3Ep = epI
		}
	}
	assert.NotNil(t, svc3Ep, "svc3's local endpoint at IP-B should still exist")
	assert.True(t, svc3Ep.IsLocal(), "svc3's endpoint should still be local")
	assert.NotEmpty(t, svc3Ep.hnsID, "svc3's local endpoint should still have a valid HNS ID")

	// 验证本地 HNS 端点并未从 HNS 中被删除。
	_, err := hcnMock.GetEndpointByID(svc3Ep.hnsID)
	assert.NoError(t, err, "Local HNS endpoint at IP-B should still exist in HNS (not deleted)")
}

// TestEndpointTransitionWithLBFailures 验证以下场景的端点引用计数：
// 端点集合从 [A,B,C,D] 变为 [A,B]，且负载均衡器删除与创建
// 操作均失败时，引用计数是否仍然正确。
//
// 流程：
//  1. 创建本地端点 A 和三个远端端点 B、C、D。
//  2. 两个服务（svc1、svc2）各引用全部 4 个端点，
//     断言远端端点 refCount 均为 2。
//  3. 注入 LB 删除失败，同时将端点从 [A,B,C,D] 变为 [A,B]；
//     旧 LB（相同 VIP:port）未被移除，导致 LB 创建也失败。
//  4. 断言 C、D 的 refCount 从 2 降为 0，B 复用 HNS 端点且 refCount 为 2，
//     A 仍为本地端点且 refCount 为 0。
//
// 该测试覆盖异常路径下引用计数与清理逻辑的正确性。
// 注入 LB 删除失败来验证异常路径下端点引用计数仍能正确收敛。
func TestEndpointTransitionWithLBFailures(t *testing.T) {
	proxier := NewFakeProxier(t, testNodeName, netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_L2BRIDGE, false)
	if proxier == nil {
		t.Fatal("Failed to create proxier")
	}

	localIP := epIpAddressLocal1   // "192.168.4.4"
	remoteIPB := epIpAddressRemote // "192.168.2.3"
	remoteIPC := "192.168.2.4"
	remoteIPD := "192.168.2.5"

	svcIP1 := "10.20.30.41"
	svcPort1 := 80
	svcNodePort1 := 3001
	svcPortName1 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	svcIP2 := "10.20.30.42"
	svcPort2 := 80
	svcNodePort2 := 3002
	svcPortName2 := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc2"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	// 步骤 1+2：创建两个服务，各含一个本地端点（A）
	// 和三个远端端点（B、C、D）。
	makeServiceMap(proxier,
		makeTestService(svcPortName1.Namespace, svcPortName1.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP1
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName1.Port,
				Port:     int32(svcPort1),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort1),
			}}
		}),
		makeTestService(svcPortName2.Namespace, svcPortName2.Name, func(svc *v1.Service) {
			svc.Spec.Type = "NodePort"
			svc.Spec.ClusterIP = svcIP2
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName2.Port,
				Port:     int32(svcPort2),
				Protocol: v1.ProtocolTCP,
				NodePort: int32(svcNodePort2),
			}}
		}),
	)

	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{Addresses: []string{localIP}, NodeName: ptr.To(testNodeName)},
				{Addresses: []string{remoteIPB}},
				{Addresses: []string{remoteIPC}},
				{Addresses: []string{remoteIPD}},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName1.Port),
				Port:     ptr.To(int32(svcPort1)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
		makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{Addresses: []string{localIP}, NodeName: ptr.To(testNodeName)},
				{Addresses: []string{remoteIPB}},
				{Addresses: []string{remoteIPC}},
				{Addresses: []string{remoteIPD}},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName2.Port),
				Port:     ptr.To(int32(svcPort2)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	// 预填充本地 HNS 端点（模拟 CNI 创建该端点）。
	hcnMock := (proxier.hcn).(*fakehcn.HcnMock)
	hcnMock.PopulateQueriedEndpoints(endpointLocal1, networkId, localIP, macAddressLocal1, prefixLen)

	proxier.setInitialized(true)
	// 首次同步后记录端点 HNS ID 与引用计数基线。
	proxier.syncProxyRules()

	// 收集首次同步后 B、C、D 的 HNS ID。
	// 对于无 overlay 的 L2Bridge，远端端点按顺序创建：
	// B → EPID-1，C → EPID-2，D → EPID-3
	// 用闭包复用端点查找逻辑，避免在多处重复遍历端点映射。
	findRemoteEp := func(svcPortName proxy.ServicePortName, ip string) *endpointInfo {
		for _, ep := range proxier.endpointsMap[svcPortName] {
			if epI, ok := ep.(*endpointInfo); ok && epI.ip == ip {
				return epI
			}
		}
		return nil
	}

	epB := findRemoteEp(svcPortName1, remoteIPB)
	epC := findRemoteEp(svcPortName1, remoteIPC)
	epD := findRemoteEp(svcPortName1, remoteIPD)
	epA := findRemoteEp(svcPortName1, localIP)

	assert.NotNil(t, epB, "Expected to find remote endpoint B")
	assert.NotNil(t, epC, "Expected to find remote endpoint C")
	assert.NotNil(t, epD, "Expected to find remote endpoint D")
	assert.NotNil(t, epA, "Expected to find local endpoint A")

	hnsIDB := epB.hnsID
	hnsIDC := epC.hnsID
	hnsIDD := epD.hnsID

	// 断言所有远端端点的 refCount 均为 2（由 svc1 和 svc2 共享）。
	assert.Equal(t, uint16(2), *proxier.endPointsRefCount[hnsIDB],
		"B refCount should be 2")
	assert.Equal(t, uint16(2), *proxier.endPointsRefCount[hnsIDC],
		"C refCount should be 2")
	assert.Equal(t, uint16(2), *proxier.endPointsRefCount[hnsIDD],
		"D refCount should be 2")

	// 记录两个服务的旧 LB ID。
	svc1Info := proxier.svcPortMap[svcPortName1].(*serviceInfo)
	svc2Info := proxier.svcPortMap[svcPortName2].(*serviceInfo)
	oldLBID1 := svc1Info.hnsID
	oldLBID2 := svc2Info.hnsID
	assert.NotEmpty(t, oldLBID1, "svc1 should have a ClusterIP LB after first sync")
	assert.NotEmpty(t, oldLBID2, "svc2 should have a ClusterIP LB after first sync")

	// 步骤 3：注入 LB 删除失败，并将端点从 [A,B,C,D] 变为 [A,B]。
	// 此后旧的负载均衡器将无法被清除。
	// 注入删除失败，模拟 HCN 删除操作失败的异常路径。
	hcnMock.ShouldFailDeleteLoadBalancer = true
	proxier.setInitialized(false)

	// 构造更新用的旧、新 EndpointSlice。
	oldSlice1 := makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{
			{Addresses: []string{localIP}, NodeName: ptr.To(testNodeName)},
			{Addresses: []string{remoteIPB}},
			{Addresses: []string{remoteIPC}},
			{Addresses: []string{remoteIPD}},
		}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(svcPortName1.Port),
			Port:     ptr.To(int32(svcPort1)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	})
	newSlice1 := makeTestEndpointSlice(svcPortName1.Namespace, svcPortName1.Name, 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{
			{Addresses: []string{localIP}, NodeName: ptr.To(testNodeName)},
			{Addresses: []string{remoteIPB}},
		}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(svcPortName1.Port),
			Port:     ptr.To(int32(svcPort1)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	})
	oldSlice2 := makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{
			{Addresses: []string{localIP}, NodeName: ptr.To(testNodeName)},
			{Addresses: []string{remoteIPB}},
			{Addresses: []string{remoteIPC}},
			{Addresses: []string{remoteIPD}},
		}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(svcPortName2.Port),
			Port:     ptr.To(int32(svcPort2)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	})
	newSlice2 := makeTestEndpointSlice(svcPortName2.Namespace, svcPortName2.Name, 1, func(eps *discovery.EndpointSlice) {
		eps.AddressType = discovery.AddressTypeIPv4
		eps.Endpoints = []discovery.Endpoint{
			{Addresses: []string{localIP}, NodeName: ptr.To(testNodeName)},
			{Addresses: []string{remoteIPB}},
		}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(svcPortName2.Port),
			Port:     ptr.To(int32(svcPort2)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}}
	})

	proxier.OnEndpointSliceUpdate(oldSlice1, newSlice1)
	proxier.OnEndpointSliceUpdate(oldSlice2, newSlice2)

	proxier.mu.Lock()
	proxier.endpointSlicesSynced = true
	proxier.mu.Unlock()

	proxier.setInitialized(true)
	// 同步后 LB 因旧 LB 未删除而创建失败，验证失败路径下的引用计数。
	proxier.syncProxyRules()

	// 步骤 4：断言结果。
	//
	// 注入 LB 删除失败，因此旧 LB 仍留在 HNS 中。
	// 由于旧 LB（相同 VIP:port）从未移除，LB 创建自然失败，
	// 并引发 "port already exists" 冲突。
	// 删除失败、创建被跳过时，hnsID 应保持为空。
	svc1Info = proxier.svcPortMap[svcPortName1].(*serviceInfo)
	svc2Info = proxier.svcPortMap[svcPortName2].(*serviceInfo)

	// svcInfo.hnsID 应为空：删除失败且创建被跳过。
	assert.Empty(t, svc1Info.hnsID, "svc1 LB ID should be empty because create failed due to existing LB")
	assert.Empty(t, svc2Info.hnsID, "svc2 LB ID should be empty because create failed due to existing LB")

	// 由于删除失败，旧 LB 应仍存在于 HNS 中。
	lb1, err1 := hcnMock.GetLoadBalancerByID(oldLBID1)
	assert.NoError(t, err1, "Old LB for svc1 should still exist in HNS (delete failed)")
	assert.NotNil(t, lb1)
	lb2, err2 := hcnMock.GetLoadBalancerByID(oldLBID2)
	assert.NoError(t, err2, "Old LB for svc2 should still exist in HNS (delete failed)")
	assert.NotNil(t, lb2)

	// C 和 D 的 refCount 应从 2 降为 0。
	// endpointsMapChange 回调使用旧端点集调用 cleanupAllPolicies
	// （回调时映射尚未更新），因此每个服务各递减一次，共两次，降为 0。
	// 它们留在 terminatedEndpoints（不在新映射中），并由 cleanupTerminatedEndpoints 删除。
	// 这一点验证了失败路径下引用计数仍能被正确清理。
	//
	assert.Equal(t, uint16(2), *proxier.endPointsRefCount[hnsIDB],
		"B refCount should be 2")
	assert.Equal(t, uint16(0), *proxier.endPointsRefCount[hnsIDC],
		"C refCount should be 0 after transition")
	assert.Equal(t, uint16(0), *proxier.endPointsRefCount[hnsIDD],
		"D refCount should be 0 after transition")

	// C 和 D 的 HNS 端点应已被 cleanupTerminatedEndpoints 删除。
	_, errC := hcnMock.GetEndpointByID(hnsIDC)
	assert.Error(t, errC, "C HNS endpoint should have been deleted")
	_, errD := hcnMock.GetEndpointByID(hnsIDD)
	assert.Error(t, errD, "D HNS endpoint should have been deleted")

	epBNew := findRemoteEp(svcPortName1, remoteIPB)
	assert.NotNil(t, epBNew, "B endpoint should exist for svc1")
	assert.Equal(t, hnsIDB, epBNew.hnsID,
		"B should keep the same HNS ID (reused, not recreated)")
	assert.Equal(t, uint16(2), *proxier.endPointsRefCount[hnsIDB],
		"B refCount should be 2 after reuse by both services")

	// A 是本地端点 —— 其私有 refCount 应为 0（本地端点不参与引用计数）。
	epANew := findRemoteEp(svcPortName1, localIP)
	assert.NotNil(t, epANew, "Local endpoint A should still exist")
	assert.True(t, epANew.IsLocal(), "A should be local")
}

// TestClassifyLBError 验证错误分类函数在 nil、通用错误及 shim 错误类型下的行为。
func TestClassifyLBError(t *testing.T) {
	// 注意：classifyLBError 使用 hcn.Is*Error 函数，这些函数检查带有特定错误码的
	// *hcn.HcnError。由于 HcnError 只能由 hcn 包内部创建，
	// 这里只能测试 nil 和通用错误两种情况。
	// 真正的 HCN 错误分类由 hcn 包自身的测试覆盖。
	tests := []struct {
		name     string
		err      error
		expected lbErrorType
	}{
		{
			name:     "nil error returns none",
			err:      nil,
			expected: lbErrNone,
		},
		{
			name:     "generic error returns other",
			err:      errors.New("something went wrong"),
			expected: lbErrOther,
		},
		{
			// hcn.NetworkNotFoundError 是 shim 错误类型，不是 HCN 系统错误。
			// hcn.IsNetworkNotFoundError 检查带有 HCN_E_NETWORK_NOT_FOUND 错误码的 HcnError，
			// 因此返回 lbErrOther。
			name:     "hcn NetworkNotFoundError shim type returns other",
			err:      hcn.NetworkNotFoundError{NetworkName: "testnet"},
			expected: lbErrOther,
		},
		{
			name:     "hcn LoadBalancerNotFoundError shim type returns other",
			err:      hcn.LoadBalancerNotFoundError{LoadBalancerId: "lb-123"},
			expected: lbErrOther,
		},
		{
			name:     "hcn EndpointNotFoundError shim type returns other",
			err:      hcn.EndpointNotFoundError{EndpointID: "ep-123"},
			expected: lbErrOther,
		},
		{
			name:     "wrapped generic error returns other",
			err:      fmt.Errorf("wrapped: %w", errors.New("inner error")),
			expected: lbErrOther,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子用例独立验证一种错误类型的分类结果。
			result := classifyLBError(tc.err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestLoadBalancerTypeConstants 验证所有负载均衡器类型常量非空且互不重复。
func TestLoadBalancerTypeConstants(t *testing.T) {
	// 验证所有 loadBalancerType 常量的字符串值均非空。
	lbTypes := []loadBalancerType{
		lbTypeClusterIP,
		lbTypeNodePort,
		lbTypeExternalIP,
		lbTypeIngressIP,
		lbTypeHealthCheck,
	}
	seen := make(map[loadBalancerType]bool)
	for _, lbt := range lbTypes {
		assert.NotEmpty(t, string(lbt), "loadBalancerType constant should not be empty")
		assert.False(t, seen[lbt], "duplicate loadBalancerType constant: %s", lbt)
		seen[lbt] = true
	}
}

// TestLBErrorTypeConstants 验证所有错误类型常量非空且互不重复。
func TestLBErrorTypeConstants(t *testing.T) {
	// 验证所有 lbErrorType 常量的字符串值非空且唯一。
	errTypes := []lbErrorType{
		lbErrNetworkNotFound,
		lbErrPortAlreadyExists,
		lbErrNotImplemented,
		lbErrInvalidIP,
		lbErrOther,
		lbErrNone,
	}
	seen := make(map[lbErrorType]bool)
	for _, et := range errTypes {
		assert.NotEmpty(t, string(et), "lbErrorType constant should not be empty")
		assert.False(t, seen[et], "duplicate lbErrorType constant: %s", et)
		seen[et] = true
	}
}

// TestLBMetricEmission 验证指标按 IP 家族、LB 类型与错误类型正确打标签并计数。
func TestLBMetricEmission(t *testing.T) {
	// 先注册指标，否则获取指标向量失败。
	metrics.RegisterMetrics(kubeproxyconfig.ProxyModeKernelspace)

	tests := []struct {
		name      string
		metricVec *basemetrics.CounterVec
		ipFamily  string
		lbType    loadBalancerType
		errType   lbErrorType
	}{
		{
			name:      "create failure with clusterip and network_not_found",
			metricVec: metrics.WinKernelLBCreateFailure,
			ipFamily:  "IPv4",
			lbType:    lbTypeClusterIP,
			errType:   lbErrNetworkNotFound,
		},
		{
			name:      "update failure with nodeport and port_already_exists",
			metricVec: metrics.WinKernelLBUpdateFailure,
			ipFamily:  "IPv4",
			lbType:    lbTypeNodePort,
			errType:   lbErrPortAlreadyExists,
		},
		{
			name:      "delete failure with ingressip and other",
			metricVec: metrics.WinKernelLBDeleteFailure,
			ipFamily:  "IPv6",
			lbType:    lbTypeIngressIP,
			errType:   lbErrOther,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试前重置计数器，避免用例之间互相污染。
			tc.metricVec.Reset()

			tc.metricVec.WithLabelValues(tc.ipFamily, string(tc.lbType), string(tc.errType)).Inc()

			val, err := testutil.GetCounterMetricValue(tc.metricVec.WithLabelValues(tc.ipFamily, string(tc.lbType), string(tc.errType)))
			assert.NoError(t, err)
			assert.Equal(t, float64(1), val)

			// 验证其他标签组合的计数未被递增。
			// 其他标签组合的计数应为 0，证明指标按标签隔离。
			val, err = testutil.GetCounterMetricValue(tc.metricVec.WithLabelValues(tc.ipFamily, string(lbTypeHealthCheck), string(lbErrInvalidIP)))
			assert.NoError(t, err)
			assert.Equal(t, float64(0), val)
		})
	}
}

// TestCreateServiceWhereFrontEndSameAsExistingLB 验证前端 IP 和端口
// 与现有负载均衡器一致时的服务创建场景。
// 检测到匹配时，应删除现有负载均衡器并按新配置重建，
// 以确保其与新服务正确关联。
// 预置的服务前端若与现有 LB 冲突，需要删除旧 LB 并按新配置重建。
func TestCreateServiceWhereFrontEndSameAsExistingLB(t *testing.T) {
	proxier := NewFakeProxier(t, "testhost", netutils.ParseIPSloppy("10.0.0.1"), NETWORK_TYPE_L2BRIDGE, true)
	if proxier == nil {
		t.Fatal("Failed to create proxier")
	}

	svcIP := "10.20.30.41"
	lbIP := "14.0.0.14"
	svcPort := 80
	svcPortName := proxy.ServicePortName{
		NamespacedName: makeNSN("ns1", "svc1"),
		Port:           "p80",
		Protocol:       v1.ProtocolTCP,
	}

	makeServiceMap(proxier,
		makeTestService(svcPortName.Namespace, svcPortName.Name, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeLoadBalancer
			svc.Spec.ClusterIP = svcIP
			svc.Spec.LoadBalancerIP = lbIP
			svc.Spec.Ports = []v1.ServicePort{{
				Name:     svcPortName.Port,
				Port:     int32(svcPort),
				Protocol: v1.ProtocolTCP,
			}}
		}),
	)

	populateEndpointSlices(proxier,
		makeTestEndpointSlice(svcPortName.Namespace, svcPortName.Name, 1, func(eps *discovery.EndpointSlice) {
			eps.AddressType = discovery.AddressTypeIPv4
			eps.Endpoints = []discovery.Endpoint{
				{
					Addresses: []string{epIpAddressLocal1}, // 本地端点 1
				},
			}
			eps.Ports = []discovery.EndpointPort{{
				Name:     ptr.To(svcPortName.Port),
				Port:     ptr.To(int32(svcPort)),
				Protocol: ptr.To(v1.ProtocolTCP),
			}}
		}),
	)

	hcn := (proxier.hcn).(*fakehcn.HcnMock)
	// 将该端点填充到缓存中，因为它是本地端点，而本地端点由 CNI 而非 kube-proxy 管理；
	// 在此填充即将其标记为本地端点。
	hcn.PopulateQueriedEndpoints(endpointLocal1, networkId, epIpAddressLocal1, macAddressLocal1, prefixLen)
	// 预置一个与待创建服务前端 IP:端口相同的负载均衡器，制造前端冲突。
	hcn.PopulateQueriedLoadbalancers(loadbalancerGuid2, svcIP, 6, uint16(svcPort), uint16(svcPort), false, endpointLocal2)

	proxier.setInitialized(true)

	// 测试 1：添加本地端点时，服务应继续使用本地端点和现有负载均衡器；
	// 若当前没有负载均衡器，则应新建一个。
	// 同步后应删除旧 LB 并新建，避免前端 IP:端口冲突。
	proxier.syncProxyRules()

	ep := proxier.endpointsMap[svcPortName][0]
	epInfo, ok := ep.(*endpointInfo)
	assert.True(t, ok, fmt.Sprintf("Failed to cast endpointInfo %q", svcPortName.String()))
	assert.NotEmpty(t, epInfo.hnsID, fmt.Sprintf("Expected HNS ID to be set for endpoint %s, but got empty value", epIpAddressRemote))

	svc := proxier.svcPortMap[svcPortName]
	svcInfo, ok := svc.(*serviceInfo)
	assert.True(t, ok, "Failed to cast serviceInfo %q", svcPortName.String())
	assert.Equal(t, svcInfo.hnsID, loadbalancerGuid1, fmt.Sprintf("%v does not match %v", svcInfo.hnsID, loadbalancerGuid1))
	lb, err := proxier.hcn.GetLoadBalancerByID(loadbalancerGuid1)
	assert.Equal(t, nil, err, fmt.Sprintf("Failed to fetch loadbalancer: %s. Error: %v", loadbalancerGuid1, err))
	assert.NotNil(t, lb, "Loadbalancer object should not be nil")
}
