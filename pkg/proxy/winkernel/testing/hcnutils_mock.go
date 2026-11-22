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

package testing

import (
	"errors"
	"fmt"

	"github.com/Microsoft/hnslib/hcn"
)

// 包级状态：模拟 HNS 的端点/负载均衡器内存映射，以及 ID 分配计数器。
var (
	epIdCounter     int
	lbIdCounter     int
	endpointMap     map[string]*hcn.HostComputeEndpoint
	loadbalancerMap map[string]*hcn.HostComputeLoadBalancer
)

// HcnMock 模拟 HNS（Host Compute Service）的行为，
// 用内存映射和可注入的失败开关替代真实的 Windows 网络调用。
type HcnMock struct {
	supportedFeatures            hcn.SupportedFeatures
	network                      *hcn.HostComputeNetwork
	ShouldFailDeleteLoadBalancer bool
	ShouldReturnHnsNotRunning    bool
	CreateLoadBalancerError      error
}

// generateEndpointGuid 生成单调递增的模拟端点 ID 与名称，便于测试中断言。
func (hcnObj HcnMock) generateEndpointGuid() (endpointId string, endpointName string) {
	epIdCounter++
	endpointId = fmt.Sprintf("EPID-%d", epIdCounter)
	endpointName = fmt.Sprintf("EPName-%d", epIdCounter)
	return
}

// generateLoadbalancerGuid 生成单调递增的模拟负载均衡器 ID。
func (hcnObj HcnMock) generateLoadbalancerGuid() (loadbalancerId string) {
	lbIdCounter++
	loadbalancerId = fmt.Sprintf("LBID-%d", lbIdCounter)
	return
}

// NewHcnMock 创建一个状态清零的 HNS 模拟实例，默认声明支持常见的 HNS 特性。
func NewHcnMock(hnsNetwork *hcn.HostComputeNetwork) *HcnMock {
	epIdCounter = 0
	lbIdCounter = 0
	endpointMap = make(map[string]*hcn.HostComputeEndpoint)
	loadbalancerMap = make(map[string]*hcn.HostComputeLoadBalancer)
	return &HcnMock{
		supportedFeatures: hcn.SupportedFeatures{
			Api: hcn.ApiSupport{
				V2: true,
			},
			DSR:           true,
			RemoteSubnet:  true,
			IPv6DualStack: true,
		},
		network: hnsNetwork,
	}
}

// PopulateQueriedEndpoints 预置查询接口返回的端点；ID 已存在时追加 IP 配置。
func (hcnObj HcnMock) PopulateQueriedEndpoints(epId, hnsId, ipAddress, mac string, prefixLen uint8) {

	// 同一端点可能被多次填充，此时只补充 IP 配置，不覆盖已有数据。
	if endpoint, ok := endpointMap[epId]; ok {
		endpoint.IpConfigurations = append(endpoint.IpConfigurations, hcn.IpConfig{
			IpAddress:    ipAddress,
			PrefixLength: prefixLen,
		})
		return
	}

	endpoint := &hcn.HostComputeEndpoint{
		Id:                 epId,
		Name:               epId,
		HostComputeNetwork: hnsId,
		IpConfigurations: []hcn.IpConfig{
			{
				IpAddress:    ipAddress,
				PrefixLength: prefixLen,
			},
		},
		MacAddress: mac,
	}

	endpointMap[endpoint.Id] = endpoint
	endpointMap[endpoint.Name] = endpoint
}

// PopulateQueriedLoadbalancers 预置查询接口返回的负载均衡器；ID 已存在时追加后端端点。
func (hcnObj HcnMock) PopulateQueriedLoadbalancers(lbID, vip string, protocol uint16, internalPort, externalPort uint16, isIPv6 bool, endpointIDs ...string) {
	if lb, ok := loadbalancerMap[lbID]; ok {
		lb.HostComputeEndpoints = append(lb.HostComputeEndpoints, endpointIDs...)
		return
	}

	lb := &hcn.HostComputeLoadBalancer{
		Id:                   lbID,
		HostComputeEndpoints: append([]string(nil), endpointIDs...),
		PortMappings: []hcn.LoadBalancerPortMapping{
			{
				Protocol:     uint32(protocol),
				InternalPort: internalPort,
				ExternalPort: externalPort,
			},
		},
	}

	if vip != "" {
		lb.FrontendVIPs = []string{vip}
	}
	if isIPv6 {
		lb.Flags |= hcn.LoadBalancerFlagsIPv6
	}

	loadbalancerMap[lbID] = lb
}

// PopulateRawQueriedLoadbalancer 将负载均衡器原样注入模拟 HNS 状态，
// 从而绕过 CreateLoadBalancer 中的校验，便于构造异常状态的测试用例。
func (hcnObj HcnMock) PopulateRawQueriedLoadbalancer(lb *hcn.HostComputeLoadBalancer) {
	loadbalancerMap[lb.Id] = lb
}

// GetNetworkByName 返回模拟网络；mock 只持有一个网络，因此忽略名称参数。
func (hcnObj HcnMock) GetNetworkByName(networkName string) (*hcn.HostComputeNetwork, error) {
	return hcnObj.network, nil
}

// GetNetworkByID 返回模拟网络；mock 只持有一个网络，因此忽略 ID 参数。
func (hcnObj HcnMock) GetNetworkByID(networkID string) (*hcn.HostComputeNetwork, error) {
	return hcnObj.network, nil
}

// ListEndpoints 列出当前模拟 HNS 中的全部端点。
func (hcnObj HcnMock) ListEndpoints() ([]hcn.HostComputeEndpoint, error) {
	var hcnEPList []hcn.HostComputeEndpoint
	for _, ep := range endpointMap {
		hcnEPList = append(hcnEPList, *ep)
	}
	return hcnEPList, nil
}

// ListEndpointsOfNetwork 列出绑定到指定网络的端点。
func (hcnObj HcnMock) ListEndpointsOfNetwork(networkId string) ([]hcn.HostComputeEndpoint, error) {
	var hcnEPList []hcn.HostComputeEndpoint
	for _, ep := range endpointMap {
		if ep.HostComputeNetwork == networkId {
			hcnEPList = append(hcnEPList, *ep)
		}
	}
	return hcnEPList, nil
}

// GetEndpointByID 按 ID 查询端点；未找到时返回与真实 HNS 一致的 EndpointNotFoundError。
func (hcnObj HcnMock) GetEndpointByID(endpointId string) (*hcn.HostComputeEndpoint, error) {
	if ep, ok := endpointMap[endpointId]; ok {
		return ep, nil
	}
	epNotFoundError := hcn.EndpointNotFoundError{EndpointID: endpointId}
	return nil, epNotFoundError
}

// GetEndpointByName 按名称查询端点；未找到时返回与真实 HNS 一致的 EndpointNotFoundError。
func (hcnObj HcnMock) GetEndpointByName(endpointName string) (*hcn.HostComputeEndpoint, error) {
	if ep, ok := endpointMap[endpointName]; ok {
		return ep, nil
	}
	epNotFoundError := hcn.EndpointNotFoundError{EndpointName: endpointName}
	return nil, epNotFoundError
}

// CreateEndpoint 在模拟 HNS 中创建端点，并模拟网络不存在与 ID/名称冲突时的错误。
func (hcnObj HcnMock) CreateEndpoint(network *hcn.HostComputeNetwork, endpoint *hcn.HostComputeEndpoint) (*hcn.HostComputeEndpoint, error) {
	if _, err := hcnObj.GetNetworkByID(network.Id); err != nil {
		return nil, err
	}
	if _, ok := endpointMap[endpoint.Id]; ok {
		return nil, fmt.Errorf("endpoint id %s already present", endpoint.Id)
	}
	if _, ok := endpointMap[endpoint.Name]; ok {
		return nil, fmt.Errorf("endpoint Name %s already present", endpoint.Name)
	}
	// HNS 会忽略请求里的 ID 和名称并自行分配，这里用递增 ID 模拟该行为。
	endpoint.Id, endpoint.Name = hcnObj.generateEndpointGuid()
	endpoint.HostComputeNetwork = network.Id
	endpointMap[endpoint.Id] = endpoint
	endpointMap[endpoint.Name] = endpoint
	return endpoint, nil
}

// CreateRemoteEndpoint 与 CreateEndpoint 行为一致：mock 不区分本地与远程端点。
func (hcnObj HcnMock) CreateRemoteEndpoint(network *hcn.HostComputeNetwork, endpoint *hcn.HostComputeEndpoint) (*hcn.HostComputeEndpoint, error) {
	return hcnObj.CreateEndpoint(network, endpoint)
}

// DeleteEndpoint 删除端点，同时清理 ID 与名称两个索引，避免查询命中残留项。
func (hcnObj HcnMock) DeleteEndpoint(endpoint *hcn.HostComputeEndpoint) error {
	if _, ok := endpointMap[endpoint.Id]; !ok {
		return hcn.EndpointNotFoundError{EndpointID: endpoint.Id}
	}
	delete(endpointMap, endpoint.Id)
	delete(endpointMap, endpoint.Name)
	return nil
}

// ListLoadBalancers 列出模拟 HNS 中的全部负载均衡器。
func (hcnObj HcnMock) ListLoadBalancers() ([]hcn.HostComputeLoadBalancer, error) {
	var hcnLBList []hcn.HostComputeLoadBalancer
	for _, lb := range loadbalancerMap {
		hcnLBList = append(hcnLBList, *lb)
	}
	return hcnLBList, nil
}

// GetLoadBalancerByID 按 ID 查询负载均衡器；可注入 HNS 未运行错误以覆盖容错路径。
func (hcnObj HcnMock) GetLoadBalancerByID(loadBalancerId string) (*hcn.HostComputeLoadBalancer, error) {
	if hcnObj.ShouldReturnHnsNotRunning {
		return nil, fmt.Errorf("HNS error: 0x6b5")
	}
	if lb, ok := loadbalancerMap[loadBalancerId]; ok {
		return lb, nil
	}
	lbNotFoundError := hcn.LoadBalancerNotFoundError{LoadBalancerId: loadBalancerId}
	return nil, lbNotFoundError
}

// CreateLoadBalancer 模拟 HNS 的创建约束：检查重复 ID 与重复端口映射，并分配新 ID。
func (hcnObj HcnMock) CreateLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer) (*hcn.HostComputeLoadBalancer, error) {
	if hcnObj.CreateLoadBalancerError != nil {
		return nil, hcnObj.CreateLoadBalancerError
	}
	if _, ok := loadbalancerMap[loadBalancer.Id]; ok {
		return nil, fmt.Errorf("LoadBalancer id %s Already Present", loadBalancer.Id)
	}
	for _, lb := range loadbalancerMap {
		portMappingMatched := lb.PortMappings != nil && lb.PortMappings[0].ExternalPort == loadBalancer.PortMappings[0].ExternalPort && lb.PortMappings[0].Protocol == loadBalancer.PortMappings[0].Protocol
		portMappingMatched = portMappingMatched && (lb.PortMappings[0].InternalPort == loadBalancer.PortMappings[0].InternalPort)
		portMappingMatched = portMappingMatched && len(lb.FrontendVIPs) != 0 && len(loadBalancer.FrontendVIPs) != 0 && lb.FrontendVIPs[0] == loadBalancer.FrontendVIPs[0]
		if portMappingMatched && ((lb.Flags & hcn.LoadBalancerFlagsIPv6) == (loadBalancer.Flags & hcn.LoadBalancerFlagsIPv6)) {
			return nil, fmt.Errorf("HNS error: 0xb7 - The specified port already exists.")
		}
		if portMappingMatched && lb.PortMappings[0].Flags&hcn.LoadBalancerPortMappingFlagsLocalRoutedVIP == 0 && lb.FrontendVIPs[0] == loadBalancer.FrontendVIPs[0] {
			return nil, fmt.Errorf("HNS error: 0xb7 - The specified port already exists.")
		}
	}
	loadBalancer.Id = hcnObj.generateLoadbalancerGuid()
	loadbalancerMap[loadBalancer.Id] = loadBalancer
	return loadBalancer, nil
}

// UpdateLoadBalancer 用给定配置覆盖指定 ID 的负载均衡器。
func (hcnObj HcnMock) UpdateLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer, hnsLbID string) (*hcn.HostComputeLoadBalancer, error) {
	if _, ok := loadbalancerMap[hnsLbID]; !ok {
		return nil, fmt.Errorf("LoadBalancer id %s Not Present", loadBalancer.Id)
	}
	loadBalancer.Id = hnsLbID
	loadbalancerMap[hnsLbID] = loadBalancer
	return loadBalancer, nil
}

// DeleteLoadBalancer 删除负载均衡器；可注入失败以测试调用方的错误处理。
func (hcnObj HcnMock) DeleteLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer) error {
	if _, ok := loadbalancerMap[loadBalancer.Id]; !ok {
		return hcn.LoadBalancerNotFoundError{LoadBalancerId: loadBalancer.Id}
	}
	if hcnObj.ShouldFailDeleteLoadBalancer {
		return fmt.Errorf("injected DeleteLoadBalancer failure for %s", loadBalancer.Id)
	}
	delete(loadbalancerMap, loadBalancer.Id)
	return nil
}

// GetSupportedFeatures 返回模拟实例声明支持的 HNS 特性集合。
func (hcnObj HcnMock) GetSupportedFeatures() hcn.SupportedFeatures {
	return hcnObj.supportedFeatures
}

// Ipv6DualStackSupported 在未声明支持 IPv6 双栈时返回错误，模拟真实 HNS 的能力探测。
func (hcnObj HcnMock) Ipv6DualStackSupported() error {
	if hcnObj.supportedFeatures.IPv6DualStack {
		return nil
	}
	return errors.New("IPV6 DualStack Not Supported")
}

// DsrSupported 在未声明支持 DSR 时返回错误，模拟真实 HNS 的能力探测。
func (hcnObj HcnMock) DsrSupported() error {
	if hcnObj.supportedFeatures.DSR {
		return nil
	}
	return errors.New("DSR Not Supported")
}

// DeleteAllHnsLoadBalancerPolicy 清空所有模拟负载均衡器，用于测试间的状态隔离。
func (hcnObj HcnMock) DeleteAllHnsLoadBalancerPolicy() {
	for k := range loadbalancerMap {
		delete(loadbalancerMap, k)
	}
}

// RemoteSubnetSupported 在未声明支持远程子网时返回错误，模拟真实 HNS 的能力探测。
func (hcnObj HcnMock) RemoteSubnetSupported() error {
	if hcnObj.supportedFeatures.RemoteSubnet {
		return nil
	}

	return errors.New("remote Subnet Not Supported")
}
