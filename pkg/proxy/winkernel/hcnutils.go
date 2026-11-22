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
	"github.com/Microsoft/hnslib/hcn"

	"k8s.io/klog/v2"
)

// HcnService 抽象了本项目实际使用的 HCN API 子集。
// 通过接口注入，可以在单测中替换真实的 HCN 调用。
type HcnService interface {
	// 网络相关操作
	GetNetworkByName(networkName string) (*hcn.HostComputeNetwork, error)
	GetNetworkByID(networkID string) (*hcn.HostComputeNetwork, error)
	// 端点相关操作
	ListEndpoints() ([]hcn.HostComputeEndpoint, error)
	ListEndpointsOfNetwork(networkId string) ([]hcn.HostComputeEndpoint, error)
	GetEndpointByID(endpointId string) (*hcn.HostComputeEndpoint, error)
	GetEndpointByName(endpointName string) (*hcn.HostComputeEndpoint, error)
	CreateEndpoint(network *hcn.HostComputeNetwork, endpoint *hcn.HostComputeEndpoint) (*hcn.HostComputeEndpoint, error)
	CreateRemoteEndpoint(network *hcn.HostComputeNetwork, endpoint *hcn.HostComputeEndpoint) (*hcn.HostComputeEndpoint, error)
	DeleteEndpoint(endpoint *hcn.HostComputeEndpoint) error
	// 负载均衡器相关操作
	ListLoadBalancers() ([]hcn.HostComputeLoadBalancer, error)
	GetLoadBalancerByID(loadBalancerId string) (*hcn.HostComputeLoadBalancer, error)
	CreateLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer) (*hcn.HostComputeLoadBalancer, error)
	UpdateLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer, hnsID string) (*hcn.HostComputeLoadBalancer, error)
	DeleteLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer) error
	// 特性检测相关操作
	GetSupportedFeatures() hcn.SupportedFeatures
	Ipv6DualStackSupported() error
	DsrSupported() error
	// 策略相关操作
	DeleteAllHnsLoadBalancerPolicy()
	RemoteSubnetSupported() error
}

// hcnImpl 是 HcnService 的真实实现，直接转发到 hnslib/hcn 包函数。
type hcnImpl struct{}

// newHcnImpl 创建 HcnService 的真实实现。
func newHcnImpl() hcnImpl {
	return hcnImpl{}
}

// GetNetworkByName 按名称查询 HCN 网络。
func (hcnObj hcnImpl) GetNetworkByName(networkName string) (*hcn.HostComputeNetwork, error) {
	return hcn.GetNetworkByName(networkName)
}

// GetNetworkByID 按 ID 查询 HCN 网络。
func (hcnObj hcnImpl) GetNetworkByID(networkID string) (*hcn.HostComputeNetwork, error) {
	return hcn.GetNetworkByID(networkID)
}

// ListEndpoints 列出主机上的全部 HCN 端点。
func (hcnObj hcnImpl) ListEndpoints() ([]hcn.HostComputeEndpoint, error) {
	return hcn.ListEndpoints()
}

// ListEndpointsOfNetwork 列出指定网络上的全部端点。
func (hcnObj hcnImpl) ListEndpointsOfNetwork(networkId string) ([]hcn.HostComputeEndpoint, error) {
	return hcn.ListEndpointsOfNetwork(networkId)
}

// GetEndpointByID 按 ID 查询 HCN 端点。
func (hcnObj hcnImpl) GetEndpointByID(endpointId string) (*hcn.HostComputeEndpoint, error) {
	return hcn.GetEndpointByID(endpointId)
}

// GetEndpointByName 按名称查询 HCN 端点。
func (hcnObj hcnImpl) GetEndpointByName(endpointName string) (*hcn.HostComputeEndpoint, error) {
	return hcn.GetEndpointByName(endpointName)
}

// CreateEndpoint 在指定网络上创建本地端点。
func (hcnObj hcnImpl) CreateEndpoint(network *hcn.HostComputeNetwork, endpoint *hcn.HostComputeEndpoint) (*hcn.HostComputeEndpoint, error) {
	return network.CreateEndpoint(endpoint)
}

// CreateRemoteEndpoint 在指定网络上创建远端端点。
func (hcnObj hcnImpl) CreateRemoteEndpoint(network *hcn.HostComputeNetwork, endpoint *hcn.HostComputeEndpoint) (*hcn.HostComputeEndpoint, error) {
	return network.CreateRemoteEndpoint(endpoint)
}

// DeleteEndpoint 删除指定的 HCN 端点。
func (hcnObj hcnImpl) DeleteEndpoint(endpoint *hcn.HostComputeEndpoint) error {
	return endpoint.Delete()
}

// ListLoadBalancers 列出主机上的全部 HCN 负载均衡器。
func (hcnObj hcnImpl) ListLoadBalancers() ([]hcn.HostComputeLoadBalancer, error) {
	return hcn.ListLoadBalancers()
}

// GetLoadBalancerByID 按 ID 查询 HCN 负载均衡器。
func (hcnObj hcnImpl) GetLoadBalancerByID(loadBalancerId string) (*hcn.HostComputeLoadBalancer, error) {
	return hcn.GetLoadBalancerByID(loadBalancerId)
}

// CreateLoadBalancer 创建 HCN 负载均衡器。
func (hcnObj hcnImpl) CreateLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer) (*hcn.HostComputeLoadBalancer, error) {
	return loadBalancer.Create()
}

// UpdateLoadBalancer 按 HNS ID 更新现有的负载均衡器。
func (hcnObj hcnImpl) UpdateLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer, hnsID string) (*hcn.HostComputeLoadBalancer, error) {
	return loadBalancer.Update(hnsID)
}

// DeleteLoadBalancer 删除指定的 HCN 负载均衡器。
func (hcnObj hcnImpl) DeleteLoadBalancer(loadBalancer *hcn.HostComputeLoadBalancer) error {
	return loadBalancer.Delete()
}

// GetSupportedFeatures 返回当前 Windows 版本支持的 HCN 特性集合。
func (hcnObj hcnImpl) GetSupportedFeatures() hcn.SupportedFeatures {
	return hcn.GetSupportedFeatures()
}

// Ipv6DualStackSupported 检查系统是否支持 IPv6 双栈。
func (hcnObj hcnImpl) Ipv6DualStackSupported() error {
	return hcn.IPv6DualStackSupported()
}

// DsrSupported 检查系统是否支持 DSR（直接服务器返回）。
func (hcnObj hcnImpl) DsrSupported() error {
	return hcn.DSRSupported()
}

// DeleteAllHnsLoadBalancerPolicy 删除主机上全部的 HNS 负载均衡器。
// 启动时调用以清理上次进程遗留的策略；单个删除失败只记录日志，避免阻断启动。
func (hcnObj hcnImpl) DeleteAllHnsLoadBalancerPolicy() {
	lbs, err := hcnObj.ListLoadBalancers()
	if err != nil {
		// 枚举失败时无法逐项清理，直接返回，保留现场等待下次重试。
		klog.V(2).ErrorS(err, "Deleting all existing loadbalancers failed.")
		return
	}
	klog.V(3).InfoS("Deleting all existing loadbalancers", "lbCount", len(lbs))
	for _, lb := range lbs {
		err = lb.Delete()
		if err != nil {
			// 个别策略删除失败不应阻断其余资源的清理。
			klog.V(2).ErrorS(err, "Error deleting existing loadbalancer", "lb", lb)
		}
	}
}

// RemoteSubnetSupported 检查系统是否支持远程子网路由。
func (hcnObj hcnImpl) RemoteSubnetSupported() error {
	return hcn.RemoteSubnetSupported()
}
