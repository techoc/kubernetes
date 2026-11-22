//go:build windows

/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"strings"
	"testing"

	"github.com/Microsoft/hnslib/hcn"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// 测试用常量：VIP、子网、Endpoint 地址及端口等参数。
const (
	sourceVip      = "192.168.1.2"
	serviceVip     = "11.0.0.1"
	addressPrefix  = "192.168.1.0/24"
	gatewayAddress = "192.168.1.1"
	// Endpoint 的 MAC 地址与各类 IPv4/IPv6 地址。
	epMacAddress       = "00-11-22-33-44-55"
	epIpAddress        = "192.168.1.3"
	epIpv6Address      = "192::3"
	epIpAddressB       = "192.168.1.4"
	epIpAddressLocal   = "192.168.5.3"
	epIpAddressLocalv6 = "192::5:3"
	// 本地与远端 Endpoint 地址，用于构造双栈和冲突场景。
	epIpAddressRemote   = "192.168.2.3"
	epIpAddressRemotev6 = "192::2:3"
	epIpAddressLocal1   = "192.168.4.4"
	epIpAddressLocal2   = "192.168.4.5"
	// Provider 地址及协议、端口常量。
	epPaAddress  = "10.0.0.3"
	protocol     = 6
	internalPort = 80
	externalPort = 32440
)

// TestGetNetworkByName 验证按名称查询 HNS 网络能返回正确 ID。
func TestGetNetworkByName(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	network, err := hns.getNetworkByName(Network.Name)
	if err != nil {
		t.Error(err)
	}

	if !strings.EqualFold(network.id, Network.Id) {
		t.Errorf("%v does not match %v", network.id, Network.Id)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestGetAllEndpointsByNetwork 验证双栈 Endpoint 能按 IP 地址被正确索引。
func TestGetAllEndpointsByNetwork(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	ipv4Config := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	ipv6Config := &hcn.IpConfig{
		IpAddress: epIpv6Address,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipv4Config, *ipv6Config},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}

	mapEndpointsInfo, _, err := hns.getAllEndpointsByNetwork(Network.Name)
	if err != nil {
		t.Error(err)
	}
	endpointIpv4, ipv4EpPresent := mapEndpointsInfo[ipv4Config.IpAddress]
	assert.True(t, ipv4EpPresent, "IPV4 endpoint is missing in Dualstack mode")
	assert.Equal(t, endpointIpv4.ip, epIpAddress, "IPV4 IP is missing in Dualstack mode")

	endpointIpv6, ipv6EpPresent := mapEndpointsInfo[ipv6Config.IpAddress]
	assert.True(t, ipv6EpPresent, "IPV6 endpoint is missing in Dualstack mode")
	assert.Equal(t, endpointIpv6.ip, epIpv6Address, "IPV6 IP is missing in Dualstack mode")

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestGetAllEndpointsByNetworkWithDupEP 验证本地与远端 Endpoint IP 冲突时的清理逻辑。
func TestGetAllEndpointsByNetworkWithDupEP(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	ipv4Config := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	ipv6Config := &hcn.IpConfig{
		IpAddress: epIpv6Address,
	}
	// 构造带远端标记的 Endpoint。
	remoteEndpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipv4Config, *ipv6Config},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
		Flags: hcn.EndpointFlagsRemoteEndpoint,
	}
	Network, _ := hcnMock.GetNetworkByName(testNetwork)
	remoteEndpoint, err := hns.hcn.CreateEndpoint(Network, remoteEndpoint)
	if err != nil {
		t.Error(err)
	}

	// 创建与远端 Endpoint 使用相同 IP 的本地 Endpoint，模拟 IP 冲突。
	dupLocalEndpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipv4Config, *ipv6Config},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}

	dupLocalEndpoint, err = hns.hcn.CreateEndpoint(Network, dupLocalEndpoint)
	if err != nil {
		t.Error(err)
	}

	// 查询时应返回冲突的远端 Endpoint，随后清理。
	mapEndpointsInfo, remoteEPsWithDupIP, err := hns.getAllEndpointsByNetwork(Network.Name)
	if err != nil {
		t.Error(err)
	}
	// 删除 IP 冲突的远端 Endpoint，只保留本地 Endpoint。
	hns.deleteAllRemoteEndpointsWithDupIP(remoteEPsWithDupIP)
	// IPv4 Endpoint 应指向本地 Endpoint 的 HNS ID。
	endpointIpv4, ipv4EpPresent := mapEndpointsInfo[ipv4Config.IpAddress]
	assert.True(t, ipv4EpPresent, "IPV4 endpoint is missing in Dualstack mode")
	assert.Equal(t, endpointIpv4.ip, epIpAddress, "IPV4 IP is missing in Dualstack mode")
	assert.Equal(t, endpointIpv4.hnsID, dupLocalEndpoint.Id, "HNS ID is not matching with local endpoint")

	// IPv6 Endpoint 同样应指向本地 Endpoint。
	endpointIpv6, ipv6EpPresent := mapEndpointsInfo[ipv6Config.IpAddress]
	assert.True(t, ipv6EpPresent, "IPV6 endpoint is missing in Dualstack mode")
	assert.Equal(t, endpointIpv6.ip, epIpv6Address, "IPV6 IP is missing in Dualstack mode")
	assert.Equal(t, endpointIpv6.hnsID, dupLocalEndpoint.Id, "HNS ID is not matching with local endpoint")

	// 远端 Endpoint 应已被删除。
	remoteEpExists, _ := hns.hcn.GetEndpointByID(remoteEndpoint.Id)
	assert.Nil(t, remoteEpExists, "Remote endpoint with duplicate IP should have been deleted")

	// 清理重复的本地 Endpoint。
	err = hns.hcn.DeleteEndpoint(dupLocalEndpoint)
	if err != nil {
		t.Error(err)
	}
}

// TestGetEndpointByID 验证按 HNS ID 查询 Endpoint。
func TestGetEndpointByID(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	ipConfig := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}

	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}

	endpoint, err := hns.getEndpointByID(Endpoint.Id)
	if err != nil {
		t.Error(err)
	}
	if !strings.EqualFold(endpoint.hnsID, Endpoint.Id) {
		t.Errorf("%v does not match %v", endpoint.hnsID, Endpoint.Id)
	}

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestGetEndpointByIpAddressAndName 验证按 IP 和名称查询 Endpoint 结果一致。
func TestGetEndpointByIpAddressAndName(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	ipConfig := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}

	endpoint, err := hns.getEndpointByIpAddress(Endpoint.IpConfigurations[0].IpAddress, Network.Name)
	if err != nil {
		t.Error(err)
	}
	if !strings.EqualFold(endpoint.hnsID, Endpoint.Id) {
		t.Errorf("%v does not match %v", endpoint.hnsID, Endpoint.Id)
	}
	if endpoint.ip != Endpoint.IpConfigurations[0].IpAddress {
		t.Errorf("%v does not match %v", endpoint.ip, Endpoint.IpConfigurations[0].IpAddress)
	}

	endpoint2, err := hns.getEndpointByName(Endpoint.Name)
	if err != nil {
		t.Error(err)
	}
	// 两种查询方式应返回相同的 Endpoint。
	diff := cmp.Diff(endpoint, endpoint2)
	if diff != "" {
		t.Errorf("getEndpointByName(%s) returned a different endpoint. Diff: %s ", Endpoint.Name, diff)
	}

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestCreateEndpointLocal 验证创建本地 Endpoint 的字段正确性。
func TestCreateEndpointLocal(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	endpoint := &endpointInfo{
		ip:         epIpAddress,
		macAddress: epMacAddress,
		isLocal:    true,
	}

	endpoint, err := hns.createEndpoint(endpoint, Network.Name)
	if err != nil {
		t.Error(err)
	}
	Endpoint, err := hcn.GetEndpointByID(endpoint.hnsID)
	if err != nil {
		t.Error(err)
	}
	if !strings.EqualFold(endpoint.hnsID, Endpoint.Id) {
		t.Errorf("%v does not match %v", endpoint.hnsID, Endpoint.Id)
	}
	if endpoint.ip != Endpoint.IpConfigurations[0].IpAddress {
		t.Errorf("%v does not match %v", endpoint.ip, Endpoint.IpConfigurations[0].IpAddress)
	}
	if endpoint.macAddress != Endpoint.MacAddress {
		t.Errorf("%v does not match %v", endpoint.macAddress, Endpoint.MacAddress)
	}

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestCreateEndpointRemote 验证创建远端 Endpoint 时会设置 Provider 地址。
func TestCreateEndpointRemote(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)
	providerAddress := epPaAddress

	endpoint := &endpointInfo{
		ip:              epIpAddressRemote,
		macAddress:      epMacAddress,
		isLocal:         false,
		providerAddress: providerAddress,
	}

	endpoint, err := hns.createEndpoint(endpoint, Network.Name)
	if err != nil {
		t.Error(err)
	}
	Endpoint, err := hcn.GetEndpointByID(endpoint.hnsID)
	if err != nil {
		t.Error(err)
	}
	if !strings.EqualFold(endpoint.hnsID, Endpoint.Id) {
		t.Errorf("%v does not match %v", endpoint.hnsID, Endpoint.Id)
	}
	if endpoint.ip != Endpoint.IpConfigurations[0].IpAddress {
		t.Errorf("%v does not match %v", endpoint.ip, Endpoint.IpConfigurations[0].IpAddress)
	}
	if endpoint.macAddress != Endpoint.MacAddress {
		t.Errorf("%v does not match %v", endpoint.macAddress, Endpoint.MacAddress)
	}
	// 远端 Endpoint 的 Provider 地址应与配置一致。
	if len(providerAddress) != 0 && endpoint.providerAddress != epPaAddress {
		t.Errorf("%v does not match %v", endpoint.providerAddress, providerAddress)
	}

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestDeleteEndpoint 验证删除 Endpoint 后无法再查询到。
func TestDeleteEndpoint(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	ipConfig := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}
	err = hns.deleteEndpoint(Endpoint.Id)
	if err != nil {
		t.Error(err)
	}
	// Endpoint 应已被删除，因此再次查询应失败。
	Endpoint, err = hcn.GetEndpointByID(Endpoint.Id)
	if err == nil {
		t.Error(err)
	}

	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestGetLoadBalancerExisting 验证存在匹配 LB 时会被复用而不是重建。
func TestGetLoadBalancerExisting(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)
	lbs := make(map[loadBalancerIdentifier]*(loadBalancerInfo))

	ipConfig := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}

	// 在 HCN 中先创建一个真实的 LB，供 getLoadBalancer 复用。
	Endpoints := []hcn.HostComputeEndpoint{*Endpoint}
	LoadBalancer, err := hcn.AddLoadBalancer(
		Endpoints,
		hcn.LoadBalancerFlagsNone,
		hcn.LoadBalancerPortMappingFlagsNone,
		sourceVip,
		[]string{serviceVip},
		protocol,
		internalPort,
		externalPort,
	)
	if err != nil {
		t.Error(err)
	}
	endpoint := &endpointInfo{
		ip:    Endpoint.IpConfigurations[0].IpAddress,
		hnsID: Endpoint.Id,
	}
	endpoints := []endpointInfo{*endpoint}
	hash, err := hashEndpoints(endpoints)
	if err != nil {
		t.Error(err)
	}

	// 预置负载 balancer 信息，用于验证“复用已有 LB”的分支。
	// 用真实的 LB ID 构造缓存条目，命中“已存在”分支。
	id := loadBalancerIdentifier{protocol: protocol, internalPort: internalPort, externalPort: externalPort, vip: serviceVip, endpointsHash: hash}
	lbs[id] = &loadBalancerInfo{hnsID: LoadBalancer.Id}

	lb, err := hns.getLoadBalancer(endpoints, loadBalancerFlags{}, sourceVip, serviceVip, protocol, internalPort, externalPort, lbs)

	if err != nil {
		t.Error(err)
	}

	if !strings.EqualFold(lb.hnsID, LoadBalancer.Id) {
		t.Errorf("%v does not match %v", lb.hnsID, LoadBalancer.Id)
	}

	err = LoadBalancer.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestGetLoadBalancerNew 验证缓存为空时会创建新的 LB。
func TestGetLoadBalancerNew(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)
	// lbs 保持为空，用于验证“新建 LB”的分支。
	lbs := make(map[loadBalancerIdentifier]*(loadBalancerInfo))

	ipConfig := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}
	endpoint := &endpointInfo{
		ip:    Endpoint.IpConfigurations[0].IpAddress,
		hnsID: Endpoint.Id,
	}
	endpoints := []endpointInfo{*endpoint}
	lb, err := hns.getLoadBalancer(endpoints, loadBalancerFlags{}, sourceVip, serviceVip, protocol, internalPort, externalPort, lbs)
	if err != nil {
		t.Error(err)
	}
	LoadBalancer, err := hcn.GetLoadBalancerByID(lb.hnsID)
	if err != nil {
		t.Error(err)
	}
	if !strings.EqualFold(lb.hnsID, LoadBalancer.Id) {
		t.Errorf("%v does not match %v", lb.hnsID, LoadBalancer.Id)
	}
	err = LoadBalancer.Delete()
	if err != nil {
		t.Error(err)
	}

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// TestDeleteLoadBalancer 验证删除 LB 后无法再查询到。
func TestDeleteLoadBalancer(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	hns := hns{hcn: newHcnImpl()}
	Network := mustTestNetwork(t)

	ipConfig := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	Endpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	Endpoint, err := Network.CreateEndpoint(Endpoint)
	if err != nil {
		t.Error(err)
	}

	Endpoints := []hcn.HostComputeEndpoint{*Endpoint}
	LoadBalancer, err := hcn.AddLoadBalancer(
		Endpoints,
		hcn.LoadBalancerFlagsNone,
		hcn.LoadBalancerPortMappingFlagsNone,
		sourceVip,
		[]string{serviceVip},
		protocol,
		internalPort,
		externalPort,
	)
	if err != nil {
		t.Error(err)
	}
	err = hns.deleteLoadBalancer(LoadBalancer.Id)
	if err != nil {
		t.Error(err)
	}
	// 负载 balancer 应已不存在。
	LoadBalancer, err = hcn.GetLoadBalancerByID(LoadBalancer.Id)
	if err == nil {
		t.Error(err)
	}

	err = Endpoint.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// mustTestNetwork 创建测试网络；创建失败时直接终止测试。
func mustTestNetwork(t *testing.T) *hcn.HostComputeNetwork {
	network, err := createTestNetwork()
	if err != nil {
		t.Fatalf("cannot create test network: %v", err)
	}
	if network == nil {
		t.Fatal("test network was nil without error")
	}
	return network
}

// TestHashEndpoints 验证 Endpoint 哈希与顺序无关。
func TestHashEndpoints(t *testing.T) {
	// TODO: remove skip once the test has been fixed.
	t.Skip("Skipping failing test on Windows.")
	Network := mustTestNetwork(t)
	// 创建 Endpoint A。
	ipConfigA := &hcn.IpConfig{
		IpAddress: epIpAddress,
	}
	endpointASpec := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfigA},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	endpointA, err := Network.CreateEndpoint(endpointASpec)
	if err != nil {
		t.Error(err)
	}
	endpointInfoA := &endpointInfo{
		ip:    endpointA.IpConfigurations[0].IpAddress,
		hnsID: endpointA.Id,
	}
	// 创建 Endpoint B。
	ipConfigB := &hcn.IpConfig{
		IpAddress: epIpAddressB,
	}
	endpointBSpec := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfigB},
		MacAddress:       epMacAddress,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}
	endpointB, err := Network.CreateEndpoint(endpointBSpec)
	if err != nil {
		t.Error(err)
	}
	endpointInfoB := &endpointInfo{
		ip:    endpointB.IpConfigurations[0].IpAddress,
		hnsID: endpointB.Id,
	}
	endpoints := []endpointInfo{*endpointInfoA, *endpointInfoB}
	// 交换 Endpoint 顺序后计算哈希。
	endpointsReverse := []endpointInfo{*endpointInfoB, *endpointInfoA}
	h1, err := hashEndpoints(endpoints)
	if err != nil {
		t.Error(err)
	} else if len(h1) < 1 {
		t.Error("HashEndpoints failed for endpoints", endpoints)
	}

	h2, err := hashEndpoints(endpointsReverse)
	if err != nil {
		t.Error(err)
	}
	// 顺序不同但内容相同，哈希应一致。
	if h1 != h2 {
		t.Errorf("%x does not match %x", h1, h2)
	}

	// 清理测试创建的资源。
	err = endpointA.Delete()
	if err != nil {
		t.Error(err)
	}
	err = endpointB.Delete()
	if err != nil {
		t.Error(err)
	}
	err = Network.Delete()
	if err != nil {
		t.Error(err)
	}
}

// createTestNetwork 构造带 VSID 策略的 Overlay 测试网络。
func createTestNetwork() (*hcn.HostComputeNetwork, error) {
	network := &hcn.HostComputeNetwork{
		Type: NETWORK_TYPE_OVERLAY,
		Name: "TestOverlay",
		MacPool: hcn.MacPool{
			Ranges: []hcn.MacRange{
				{
					StartMacAddress: "00-15-5D-52-C0-00",
					EndMacAddress:   "00-15-5D-52-CF-FF",
				},
			},
		},
		Ipams: []hcn.Ipam{
			{
				Type: "Static",
				Subnets: []hcn.Subnet{
					{
						IpAddressPrefix: addressPrefix,
						Routes: []hcn.Route{
							{
								NextHop:           gatewayAddress,
								DestinationPrefix: "0.0.0.0/0",
							},
						},
					},
				},
			},
		},
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}

	// 为子网附加 VSID 策略，隔离测试网络流量。
	vsid := &hcn.VsidPolicySetting{
		IsolationId: 5000,
	}
	vsidJson, err := json.Marshal(vsid)
	if err != nil {
		return nil, err
	}

	sp := &hcn.SubnetPolicy{
		Type: hcn.VSID,
	}
	sp.Settings = vsidJson

	spJson, err := json.Marshal(sp)
	if err != nil {
		return nil, err
	}

	// 将策略序列化后挂到子网配置上。
	network.Ipams[0].Subnets[0].Policies = append(network.Ipams[0].Subnets[0].Policies, spJson)

	return network.Create()
}

// TestIsHnsNotRunningError 验证 HNS 未运行错误的识别规则。
func TestIsHnsNotRunningError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// nil 输入不应被识别为 HNS 未运行错误。
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		// Win32 错误码 0x6b5 表示接口未知，即 HNS 未运行。
		{
			name:     "HNS not running error during enumerate endpoints",
			err:      fmt.Errorf("hcnEnumerateEndpoints failed in Win32: The interface is unknown. (0x6b5)"),
			expected: true,
		},
		{
			name:     "HNS not running error during enumerate networks",
			err:      fmt.Errorf("hcnEnumerateNetworks failed in Win32: The interface is unknown. (0x6b5)"),
			expected: true,
		},
		// 无关错误不应误判。
		{
			name:     "unrelated error",
			err:      fmt.Errorf("some other error"),
			expected: false,
		},
		// 只有 0x6b5 才算 HNS 未运行，其他错误码不算。
		{
			name:     "error containing 0xb7 but not 0x6b5",
			err:      fmt.Errorf("file already exists: 0xb7"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsHnsNotRunningError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsPolicyAlreadyExists 验证 0xb7（已存在）错误的识别规则。
func TestIsPolicyAlreadyExists(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// nil 输入不应被识别为策略已存在错误。
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		// 0xb7 表示指定的端口/策略已存在。
		{
			name:     "file already exists error",
			err:      fmt.Errorf("HNS error: 0xb7 - The specified port already exists."),
			expected: true,
		},
		// 无关错误不应误判。
		{
			name:     "unrelated error",
			err:      fmt.Errorf("some other error"),
			expected: false,
		},
		// HNS 未运行错误（0x6b5）不等于策略已存在。
		{
			name:     "HNS not running error (not file exists)",
			err:      fmt.Errorf("HNS call failed: 0x6b5"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPolicyAlreadyExists(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFindExistingLBIdByFrontend 验证按前端配置查找已有 LB 时的歧义处理。
func TestFindExistingLBIdByFrontend(t *testing.T) {
	tests := []struct {
		name        string
		proposedLB  *hcn.HostComputeLoadBalancer
		existingLBs map[loadBalancerIdentifier]*loadBalancerInfo
		expectedID  string
	}{
		// 唯一匹配时返回该 LB 的 ID。
		{
			name: "single matching LB found",
			proposedLB: &hcn.HostComputeLoadBalancer{
				FrontendVIPs: []string{serviceVip},
				PortMappings: []hcn.LoadBalancerPortMapping{
					{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
				},
				Flags: hcn.LoadBalancerFlagsNone,
			},
			existingLBs: map[loadBalancerIdentifier]*loadBalancerInfo{
				{vip: serviceVip, protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false}: {hnsID: "lb-123"},
			},
			expectedID: "lb-123",
		},
		// VIP 不匹配时不应返回任何 ID。
		{
			name: "no matching LB",
			proposedLB: &hcn.HostComputeLoadBalancer{
				FrontendVIPs: []string{serviceVip},
				PortMappings: []hcn.LoadBalancerPortMapping{
					{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
				},
				Flags: hcn.LoadBalancerFlagsNone,
			},
			existingLBs: map[loadBalancerIdentifier]*loadBalancerInfo{
				{vip: "10.0.0.99", protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false}: {hnsID: "lb-999"},
			},
			expectedID: "",
		},
		// 多个候选 LB 会产生歧义，此时返回空字符串让其重建。
		{
			name: "multiple matching LBs returns empty (ambiguous)",
			proposedLB: &hcn.HostComputeLoadBalancer{
				FrontendVIPs: []string{serviceVip},
				PortMappings: []hcn.LoadBalancerPortMapping{
					{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
				},
				Flags: hcn.LoadBalancerFlagsNone,
			},
			existingLBs: map[loadBalancerIdentifier]*loadBalancerInfo{
				{vip: serviceVip, protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false}:                             {hnsID: "lb-1"},
				{vip: serviceVip, protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false, endpointsHash: [20]byte{1}}: {hnsID: "lb-2"},
			},
			expectedID: "",
		},
		// 缓存为空时同样返回空字符串。
		{
			name: "empty existing LBs map",
			proposedLB: &hcn.HostComputeLoadBalancer{
				FrontendVIPs: []string{serviceVip},
				PortMappings: []hcn.LoadBalancerPortMapping{
					{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
				},
				Flags: hcn.LoadBalancerFlagsNone,
			},
			existingLBs: map[loadBalancerIdentifier]*loadBalancerInfo{},
			expectedID:  "",
		},
		// IPv6 标记必须与缓存条目的 isIPv6 一致。
		{
			name: "IPv6 LB matches only IPv6 entry",
			proposedLB: &hcn.HostComputeLoadBalancer{
				FrontendVIPs: []string{"fd00::1"},
				PortMappings: []hcn.LoadBalancerPortMapping{
					{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
				},
				Flags: LoadBalancerFlagsIPv6,
			},
			existingLBs: map[loadBalancerIdentifier]*loadBalancerInfo{
				{vip: "fd00::1", protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: true}:  {hnsID: "lb-v6"},
				{vip: "fd00::1", protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false}: {hnsID: "lb-v4"},
			},
			expectedID: "lb-v6",
		},
		// 空的 FrontendVIPs 应匹配 VIP 为空的 NodePort LB。
		{
			name: "proposed LB with empty FrontendVIPs matches empty vip entry",
			proposedLB: &hcn.HostComputeLoadBalancer{
				FrontendVIPs: []string{},
				PortMappings: []hcn.LoadBalancerPortMapping{
					{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
				},
				Flags: hcn.LoadBalancerFlagsNone,
			},
			existingLBs: map[loadBalancerIdentifier]*loadBalancerInfo{
				{vip: "", protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false}: {hnsID: "lb-nodeport"},
			},
			expectedID: "lb-nodeport",
		},
	}

	// 逐个用例验证查找结果。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findExistingLBIdByFrontend(tt.proposedLB, tt.existingLBs)
			assert.Equal(t, tt.expectedID, result)
		})
	}
}

// TestCreateOrReplaceLoadbalancer_Success 验证无冲突时创建 LB 成功。
func TestCreateOrReplaceLoadbalancer_Success(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	proposedLB := &hcn.HostComputeLoadBalancer{
		FrontendVIPs: []string{serviceVip},
		PortMappings: []hcn.LoadBalancerPortMapping{
			{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
		},
		Flags: hcn.LoadBalancerFlagsNone,
	}
	// 缓存为空，走正常创建路径。
	existingLBs := map[loadBalancerIdentifier]*loadBalancerInfo{}

	lb, err := hns.createOrReplaceLoadbalancer(proposedLB, existingLBs)
	assert.NoError(t, err)
	assert.NotNil(t, lb)
	assert.NotEmpty(t, lb.Id)
}

// TestCreateOrReplaceLoadbalancer_FileAlreadyExistsAndRetrySucceeds 验证 0xb7 冲突后可删除旧 LB 并重试成功。
func TestCreateOrReplaceLoadbalancer_FileAlreadyExistsAndRetrySucceeds(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	// 先创建一个会产生冲突的 LB。
	existingLB := &hcn.HostComputeLoadBalancer{
		FrontendVIPs: []string{serviceVip},
		PortMappings: []hcn.LoadBalancerPortMapping{
			{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
		},
		Flags: hcn.LoadBalancerFlagsNone,
	}
	// 先在 mock 中创建相同的 LB，制造 0xb7 冲突。
	createdLB, err := hcnMock.CreateLoadBalancer(existingLB)
	assert.NoError(t, err)

	// 构造重试逻辑用于查找冲突 LB 的本地缓存。
	existingLBs := map[loadBalancerIdentifier]*loadBalancerInfo{
		{vip: serviceVip, protocol: protocol, internalPort: internalPort, externalPort: externalPort, isIPv6: false}: {hnsID: createdLB.Id},
	}

	// 再次创建相同的 LB：应收到 0xb7，找到旧 LB，删除后重试成功。
	proposedLB := &hcn.HostComputeLoadBalancer{
		FrontendVIPs: []string{serviceVip},
		PortMappings: []hcn.LoadBalancerPortMapping{
			{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
		},
		Flags: hcn.LoadBalancerFlagsNone,
	}

	// 重试应生成不同于旧 LB 的新 ID。
	lb, err := hns.createOrReplaceLoadbalancer(proposedLB, existingLBs)
	assert.NoError(t, err)
	assert.NotNil(t, lb)
	assert.NotEqual(t, createdLB.Id, lb.Id, "should have created a new LB after deleting the old one")
}

// TestCreateOrReplaceLoadbalancer_FileAlreadyExistsNoMatchingLB 验证找不到冲突 LB 时返回原始错误。
func TestCreateOrReplaceLoadbalancer_FileAlreadyExistsNoMatchingLB(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	// 先在 HCN 层创建一个会造成冲突的 LB。
	existingLB := &hcn.HostComputeLoadBalancer{
		FrontendVIPs: []string{serviceVip},
		PortMappings: []hcn.LoadBalancerPortMapping{
			{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
		},
		Flags: hcn.LoadBalancerFlagsNone,
	}
	// 制造冲突但不把该 LB 放入本地缓存。
	_, err := hcnMock.CreateLoadBalancer(existingLB)
	assert.NoError(t, err)

	// 本地缓存中没有该 LB，无法通过 frontend 找到可删除的对象。
	existingLBs := map[loadBalancerIdentifier]*loadBalancerInfo{}

	proposedLB := &hcn.HostComputeLoadBalancer{
		FrontendVIPs: []string{serviceVip},
		PortMappings: []hcn.LoadBalancerPortMapping{
			{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
		},
		Flags: hcn.LoadBalancerFlagsNone,
	}

	// 无法删除旧 LB，应返回原始的 0xb7 错误。
	lb, err := hns.createOrReplaceLoadbalancer(proposedLB, existingLBs)
	assert.Error(t, err, "should return the original 0xb7 error since no matching LB was found to delete")
	assert.Nil(t, lb)
}

// TestGetAllLoadBalancers_SkipsLoadBalancersWithoutPortMappings 验证没有端口映射的畸形 LB 会被跳过。
func TestGetAllLoadBalancers_SkipsLoadBalancersWithoutPortMappings(t *testing.T) {
	testCases := []struct {
		name         string
		portMappings []hcn.LoadBalancerPortMapping
	}{
		// nil 与空切片都视为无效端口映射。
		{
			name:         "nil port mappings",
			portMappings: nil,
		},
		{
			name:         "empty port mappings",
			portMappings: []hcn.LoadBalancerPortMapping{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个用例都从干净的 mock 开始，避免状态串扰。
			hcnMock := getHcnMock("L2Bridge")
			hns := hns{hcn: hcnMock}

			// 注入一个没有端口映射的畸形 LB。
			hcnMock.PopulateRawQueriedLoadbalancer(&hcn.HostComputeLoadBalancer{
				Id:           "LBID-malformed",
				FrontendVIPs: []string{serviceVip},
				PortMappings: tc.portMappings,
			})
			// 再注入一个正常的 LB，作为应为唯一保留结果的对照。
			hcnMock.PopulateQueriedLoadbalancers("LBID-valid", serviceVip, protocol, internalPort, externalPort, false, "EPID-1")

			loadBalancers, err := hns.getAllLoadBalancers()
			assert.NoError(t, err)

			// 收集返回结果，只断言有效 LB 被保留。
			hnsIDs := make([]string, 0, len(loadBalancers))
			for _, lb := range loadBalancers {
				hnsIDs = append(hnsIDs, lb.hnsID)
			}
			assert.ElementsMatch(t, []string{"LBID-valid"}, hnsIDs)
		})
	}
}

// TestGetAllLoadBalancers_AllLoadBalancersWithoutPortMappings 验证全部 LB 无效时返回空列表。
func TestGetAllLoadBalancers_AllLoadBalancersWithoutPortMappings(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	hcnMock.PopulateRawQueriedLoadbalancer(&hcn.HostComputeLoadBalancer{Id: "LBID-malformed"})

	loadBalancers, err := hns.getAllLoadBalancers()
	assert.NoError(t, err)
	assert.Empty(t, loadBalancers)
}

// TestDeleteLoadBalancer_NotFound 验证删除不存在的 LB 是幂等的。
func TestDeleteLoadBalancer_NotFound(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	// 删除不存在的 LB 应静默返回 nil。
	err := hns.deleteLoadBalancer("non-existent-lb-id")
	assert.NoError(t, err, "deleteLoadBalancer should return nil for not-found errors")
}

// TestDeleteLoadBalancer_HnsNotRunning 验证 HNS 未运行时删除 LB 会传播错误。
func TestDeleteLoadBalancer_HnsNotRunning(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	// 配置 mock 返回 HNS 未运行错误。
	hcnMock.ShouldReturnHnsNotRunning = true
	hns := hns{hcn: hcnMock}

	err := hns.deleteLoadBalancer("any-lb-id")
	assert.Error(t, err, "deleteLoadBalancer should return error when HNS is not running")
	assert.True(t, IsHnsNotRunningError(err))
}

// TestDeleteLoadBalancer_Success 验证删除存在的 LB 成功。
func TestDeleteLoadBalancer_Success(t *testing.T) {
	hcnMock := getHcnMock("L2Bridge")
	hns := hns{hcn: hcnMock}

	// 先创建一个 LB。
	lb := &hcn.HostComputeLoadBalancer{
		FrontendVIPs: []string{serviceVip},
		PortMappings: []hcn.LoadBalancerPortMapping{
			{Protocol: protocol, InternalPort: internalPort, ExternalPort: externalPort},
		},
		Flags: hcn.LoadBalancerFlagsNone,
	}
	createdLB, err := hcnMock.CreateLoadBalancer(lb)
	assert.NoError(t, err)

	// 删除刚创建的 LB。
	err = hns.deleteLoadBalancer(createdLB.Id)
	assert.NoError(t, err)

	// 确认 LB 已不存在。
	_, err = hcnMock.GetLoadBalancerByID(createdLB.Id)
	assert.Error(t, err, "LB should have been deleted")
}
