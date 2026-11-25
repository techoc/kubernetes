/*
Copyright 2016 The Kubernetes Authors.

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

package discovery

import (
	"net"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Addresses 决定在 discovery 响应中向不同客户端返回哪个 apiserver 地址。
//
// 集群内外的客户端可能通过不同入口访问 API（例如外部 LB 与内部 Service），
// 因此响应会携带一组 (CIDR, ServerAddress) 供客户端自行挑选最匹配的项。
type Addresses interface {
	ServerAddressByClientCIDRs(net.IP) []metav1.ServerAddressByClientCIDR
}

// DefaultAddresses 是 Addresses 的默认实现，适用于大多数场景
type DefaultAddresses struct {
	// CIDRRules 是一组 CIDR 与地址规则：客户端 IP 落在某个网段内时使用对应地址
	CIDRRules []CIDRRule

	// DefaultAddress 是没有任何 CIDR 规则更精确匹配时应使用的地址
	// （主机名，或 IP 加端口）
	DefaultAddress string
}

// CIDRRule 基于 CIDR 匹配，为访问 apiserver 增加一条备用地址路径
type CIDRRule struct {
	IPRange net.IPNet

	// Address 是该 CIDR 匹配时应使用的地址（主机名，或 IP 加端口）
	Address string
}

// ServerAddressByClientCIDRs 返回兜底的 0.0.0.0/0 地址加上所有匹配到的 CIDR 规则。
//
// 兜底项放在首位，保证即使没有任何规则匹配，客户端也总能拿到一个可用地址；
// 规则之间保持配置顺序，不做最长前缀匹配——由客户端负责选择。
func (d DefaultAddresses) ServerAddressByClientCIDRs(clientIP net.IP) []metav1.ServerAddressByClientCIDR {
	addressCIDRMap := []metav1.ServerAddressByClientCIDR{
		{
			ClientCIDR:    "0.0.0.0/0",
			ServerAddress: d.DefaultAddress,
		},
	}

	for _, rule := range d.CIDRRules {
		addressCIDRMap = append(addressCIDRMap, rule.ServerAddressByClientCIDRs(clientIP)...)
	}
	return addressCIDRMap
}

// ServerAddressByClientCIDRs 仅在 clientIP 落在该规则网段内时返回条目，否则返回空。
func (d CIDRRule) ServerAddressByClientCIDRs(clientIP net.IP) []metav1.ServerAddressByClientCIDR {
	addressCIDRMap := []metav1.ServerAddressByClientCIDR{}

	if d.IPRange.Contains(clientIP) {
		addressCIDRMap = append(addressCIDRMap, metav1.ServerAddressByClientCIDR{
			ClientCIDR:    d.IPRange.String(),
			ServerAddress: d.Address,
		})
	}
	return addressCIDRMap
}
