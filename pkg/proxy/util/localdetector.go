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

package util

import (
	netutils "k8s.io/utils/net"
)

// LocalTrafficDetector 生成用于识别来自本地 Pod 流量的 iptables 或 nftables 规则参数。
type LocalTrafficDetector interface {
	// IsImplemented 在实现会生成规则时返回 true，否则返回 false。
	// 返回 false 时不应调用其余方法。
	IsImplemented() bool

	// IfLocal 返回匹配来自本地 Pod 流量的 iptables 参数。
	IfLocal() []string

	// IfNotLocal 返回匹配非本地 Pod 流量的 iptables 参数。
	IfNotLocal() []string

	// IfLocalNFT 返回匹配来自本地 Pod 流量的 nftables 参数。
	IfLocalNFT() []string

	// IfNotLocalNFT 返回匹配非本地 Pod 流量的 nftables 参数。
	IfNotLocalNFT() []string
}

// detectLocal 缓存以 iptables 与 nftables 参数形式表示的本地流量判定条件。
type detectLocal struct {
	ifLocal       []string
	ifNotLocal    []string
	ifLocalNFT    []string
	ifNotLocalNFT []string
}

// IsImplemented 根据是否存在本地匹配参数判断探测器是否实际生效。
func (d *detectLocal) IsImplemented() bool {
	return len(d.ifLocal) > 0
}

// IfLocal 返回匹配本地 Pod 流量的 iptables 参数。
func (d *detectLocal) IfLocal() []string {
	return d.ifLocal
}

// IfNotLocal 返回匹配非本地 Pod 流量的 iptables 参数。
func (d *detectLocal) IfNotLocal() []string {
	return d.ifNotLocal
}

// IfLocalNFT 返回匹配本地 Pod 流量的 nftables 参数。
func (d *detectLocal) IfLocalNFT() []string {
	return d.ifLocalNFT
}

// IfNotLocalNFT 返回匹配非本地 Pod 流量的 nftables 参数。
func (d *detectLocal) IfNotLocalNFT() []string {
	return d.ifNotLocalNFT
}

// NewNoOpLocalDetector 返回一个不生成任何规则的 LocalTrafficDetector。
func NewNoOpLocalDetector() LocalTrafficDetector {
	return &detectLocal{}
}

// NewDetectLocalByCIDR 返回一个将来自 cidr 的流量视为本地 Pod 流量的探测器，
// 其余流量视为非本地流量；cidr 应当是合法值。
func NewDetectLocalByCIDR(cidr string) LocalTrafficDetector {
	// nftables 要求显式指定地址族，而 iptables 的 -s 参数本身不区分 IPv4 与 IPv6。
	nftFamily := "ip"
	if netutils.IsIPv6CIDRString(cidr) {
		nftFamily = "ip6"
	}

	return &detectLocal{
		ifLocal:       []string{"-s", cidr},
		ifNotLocal:    []string{"!", "-s", cidr},
		ifLocalNFT:    []string{nftFamily, "saddr", cidr},
		ifNotLocalNFT: []string{nftFamily, "saddr", "!=", cidr},
	}
}

// NewDetectLocalByBridgeInterface 返回一个将来自 interfaceName 的流量视为本地 Pod 流量的探测器，
// 来自其他接口的流量则被视为非本地流量。
func NewDetectLocalByBridgeInterface(interfaceName string) LocalTrafficDetector {
	return &detectLocal{
		ifLocal:       []string{"-i", interfaceName},
		ifNotLocal:    []string{"!", "-i", interfaceName},
		ifLocalNFT:    []string{"iifname", interfaceName},
		ifNotLocalNFT: []string{"iifname", "!=", interfaceName},
	}
}

// NewDetectLocalByInterfaceNamePrefix 返回一个将来自以 interfacePrefix 开头的接口的流量视为本地 Pod 流量的探测器，
// 来自其他接口的流量则被视为非本地流量。
func NewDetectLocalByInterfaceNamePrefix(interfacePrefix string) LocalTrafficDetector {
	// iptables 的 "+" 与 nftables 的 "*" 都是后缀通配符：veth 等接口名带有后缀，
	// 只写前缀无法匹配，因此必须在前缀后追加通配符。
	return &detectLocal{
		ifLocal:       []string{"-i", interfacePrefix + "+"},
		ifNotLocal:    []string{"!", "-i", interfacePrefix + "+"},
		ifLocalNFT:    []string{"iifname", interfacePrefix + "*"},
		ifNotLocalNFT: []string{"iifname", "!=", interfacePrefix + "*"},
	}
}
