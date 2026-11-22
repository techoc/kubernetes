/*
Copyright 2019 The Kubernetes Authors.

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

package metaproxier

import (
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
)

// metaProxier 将两个单栈 proxier 包装为一个双栈 Provider：
// Service 类事件广播给两者，端点类事件则按地址族分发给对应实现。
type metaProxier struct {
	// 被包装的 IPv4 实际 proxier。
	ipv4Proxier proxy.Provider
	// 被包装的 IPv6 实际 proxier。
	ipv6Proxier proxy.Provider
}

// NewMetaProxier 返回一个双栈「元 proxier」。
// 对 Proxier API 的调用会根据地址族分发到对应的 ProxyProvider 实例，
// 使调用方无需感知底层是两个独立的单栈实现。
func NewMetaProxier(ipv4Proxier, ipv6Proxier proxy.Provider) proxy.Provider {
	return proxy.Provider(&metaProxier{
		ipv4Proxier: ipv4Proxier,
		ipv6Proxier: ipv6Proxier,
	})
}

// Sync 立即将两个 ProxyProvider 的当前状态同步到转发规则。
func (proxier *metaProxier) Sync() {
	proxier.ipv4Proxier.Sync()
	proxier.ipv6Proxier.Sync()
}

// SyncLoop 执行周期性任务，作为 goroutine 或应用主循环运行，不会返回。
func (proxier *metaProxier) SyncLoop() {
	// IPv6 的循环放入独立 goroutine，因为 IPv4 循环永不返回，
	// 串行调用会导致双栈中只有一个地址族得到服务。
	go proxier.ipv6Proxier.SyncLoop() // 此处必须使用 goroutine！
	proxier.ipv4Proxier.SyncLoop()    // 永不返回
}

// OnServiceAdd 在观察到新 Service 对象创建时调用。
// Service 不区分地址族，因此同时通知两个 proxier。
func (proxier *metaProxier) OnServiceAdd(service *v1.Service) {
	proxier.ipv4Proxier.OnServiceAdd(service)
	proxier.ipv6Proxier.OnServiceAdd(service)
}

// OnServiceUpdate 在观察到已有 Service 被修改时调用，同样广播给两个 proxier。
func (proxier *metaProxier) OnServiceUpdate(oldService, service *v1.Service) {
	proxier.ipv4Proxier.OnServiceUpdate(oldService, service)
	proxier.ipv6Proxier.OnServiceUpdate(oldService, service)
}

// OnServiceDelete 在观察到已有 Service 被删除时调用，同样广播给两个 proxier。
func (proxier *metaProxier) OnServiceDelete(service *v1.Service) {
	proxier.ipv4Proxier.OnServiceDelete(service)
	proxier.ipv6Proxier.OnServiceDelete(service)

}

// OnServiceSynced 在所有初始事件处理完成、状态完全同步到本地缓存后调用。
func (proxier *metaProxier) OnServiceSynced() {
	proxier.ipv4Proxier.OnServiceSynced()
	proxier.ipv6Proxier.OnServiceSynced()
}

// OnEndpointSliceAdd 在观察到新 EndpointSlice 创建时调用。
// 按地址类型分发，避免 IPv4 端点进入 IPv6 规则集。
func (proxier *metaProxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
	switch endpointSlice.AddressType {
	case discovery.AddressTypeIPv4:
		proxier.ipv4Proxier.OnEndpointSliceAdd(endpointSlice)
	case discovery.AddressTypeIPv6:
		proxier.ipv6Proxier.OnEndpointSliceAdd(endpointSlice)
	default:
		klog.ErrorS(nil, "EndpointSlice address type not supported", "addressType", endpointSlice.AddressType)
	}
}

// OnEndpointSliceUpdate 在观察到已有 EndpointSlice 被修改时调用，按地址类型分发。
func (proxier *metaProxier) OnEndpointSliceUpdate(oldEndpointSlice, newEndpointSlice *discovery.EndpointSlice) {
	switch newEndpointSlice.AddressType {
	case discovery.AddressTypeIPv4:
		proxier.ipv4Proxier.OnEndpointSliceUpdate(oldEndpointSlice, newEndpointSlice)
	case discovery.AddressTypeIPv6:
		proxier.ipv6Proxier.OnEndpointSliceUpdate(oldEndpointSlice, newEndpointSlice)
	default:
		klog.ErrorS(nil, "EndpointSlice address type not supported", "addressType", newEndpointSlice.AddressType)
	}
}

// OnEndpointSliceDelete 在观察到已有 EndpointSlice 被删除时调用，按地址类型分发。
func (proxier *metaProxier) OnEndpointSliceDelete(endpointSlice *discovery.EndpointSlice) {
	switch endpointSlice.AddressType {
	case discovery.AddressTypeIPv4:
		proxier.ipv4Proxier.OnEndpointSliceDelete(endpointSlice)
	case discovery.AddressTypeIPv6:
		proxier.ipv6Proxier.OnEndpointSliceDelete(endpointSlice)
	default:
		klog.ErrorS(nil, "EndpointSlice address type not supported", "addressType", endpointSlice.AddressType)
	}
}

// OnEndpointSlicesSynced 在所有初始 EndpointSlice 事件处理完成、状态同步到本地缓存后调用。
func (proxier *metaProxier) OnEndpointSlicesSynced() {
	proxier.ipv4Proxier.OnEndpointSlicesSynced()
	proxier.ipv6Proxier.OnEndpointSlicesSynced()
}

// OnTopologyChange 在观察到与代理相关的拓扑标签变化时调用，两个地址族均需更新。
func (proxier *metaProxier) OnTopologyChange(topologyLabels map[string]string) {
	proxier.ipv4Proxier.OnTopologyChange(topologyLabels)
	proxier.ipv6Proxier.OnTopologyChange(topologyLabels)
}

// OnServiceCIDRsChanged 在 ServiceCIDR 发生变化时调用，参数为完整的 CIDR 列表。
// 两个单栈 proxier 需各自从中筛选出属于自己地址族的部分。
func (proxier *metaProxier) OnServiceCIDRsChanged(cidrs []string) {
	proxier.ipv4Proxier.OnServiceCIDRsChanged(cidrs)
	proxier.ipv6Proxier.OnServiceCIDRsChanged(cidrs)
}
