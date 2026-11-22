/*
Copyright 2022 The Kubernetes Authors.

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
	"fmt"
	"net"

	v1 "k8s.io/api/core/v1"
	netutils "k8s.io/utils/net"
)

// NodePortAddresses 用于处理 --nodeport-addresses 命令行参数。
type NodePortAddresses struct {
	cidrStrings []string

	cidrs                    []*net.IPNet
	containsIPv4Loopback     bool
	containsExplicitLoopback bool
	matchAll                 bool
}

// RFC 5735 127.0.0.0/8 —— 该地址段被分配用作互联网主机回环地址。
var ipv4LoopbackStart = net.IPv4(127, 0, 0, 0)

// NewNodePortAddresses 接收 IP 族和 `--nodeport-addresses` 的取值（假定只包含合法 CIDR，
// 且可能同时包含两个 IP 族），返回对应 IP 族的 NodePortAddresses 对象。若没有该族的 CIDR，
// 则自动加入 "0.0.0.0/0" 或 "::/0"（即便参数中存在另一族的 CIDR）。
func NewNodePortAddresses(family v1.IPFamily, cidrStrings []string) *NodePortAddresses {
	npa := &NodePortAddresses{}

	// 先按目标 IP 族过滤 CIDR，避免把无关族的地址用于本族的匹配。
	for _, str := range cidrStrings {
		if (family == v1.IPv4Protocol) == netutils.IsIPv4CIDRString(str) {
			npa.cidrStrings = append(npa.cidrStrings, str)
		}
	}
	// 过滤后为空时用通配 CIDR 兜底，保证本族节点 IP 始终能被匹配到。
	if len(npa.cidrStrings) == 0 {
		if family == v1.IPv4Protocol {
			npa.cidrStrings = []string{IPv4ZeroCIDR}
		} else {
			npa.cidrStrings = []string{IPv6ZeroCIDR}
		}
	}

	// 单独扫描原始字符串：显式回环 CIDR 与“恰好覆盖回环地址”的 CIDR 需要区别对待。
	for _, str := range npa.cidrStrings {
		if _, cidr, err := netutils.ParseCIDRSloppy(str); err == nil && cidr.IP.IsLoopback() {
			npa.containsExplicitLoopback = true
			break
		}
	}

	// 解析过滤后的 CIDR。
	for _, str := range npa.cidrStrings {
		_, cidr, _ := netutils.ParseCIDRSloppy(str)

		if netutils.IsIPv4CIDR(cidr) {
			// CIDR 可以直接指向回环地址，也可以覆盖 127.0.0.0/8 的起始地址，两种情况都要标记。
			if cidr.IP.IsLoopback() || cidr.Contains(ipv4LoopbackStart) {
				npa.containsIPv4Loopback = true
			}
		}

		if IsZeroCIDR(cidr) {
			// 通配 CIDR 已覆盖所有地址，其余 CIDR 再无意义，直接忽略并结束解析。
			npa.cidrs = []*net.IPNet{cidr}
			npa.matchAll = true
			break
		}

		npa.cidrs = append(npa.cidrs, cidr)
	}

	return npa
}

// String 返回 npa 中 CIDR 字符串列表的字符串表示。
func (npa *NodePortAddresses) String() string {
	return fmt.Sprintf("%v", npa.cidrStrings)
}

// MatchAll 在 npa 匹配（其所属 IP 族的）所有节点 IP 时返回 true。
func (npa *NodePortAddresses) MatchAll() bool {
	return npa.matchAll
}

// GetNodeIPs 返回 npa 的 CIDR 所匹配的全部节点 IP 地址；若没有匹配的 IP，则返回空列表。
// NetworkInterfacer 是出于测试目的注入的。
func (npa *NodePortAddresses) GetNodeIPs(nw NetworkInterfacer) ([]net.IP, error) {
	addrs, err := nw.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("error listing all interfaceAddrs from host, error: %v", err)
	}

	// 用 map 去重：同一个 IP 可能同时匹配多个 CIDR，也可能在同一接口上重复出现。
	addresses := make(map[string]net.IP)
	for _, cidr := range npa.cidrs {
		for _, addr := range addrs {
			var ip net.IP
			// nw.InterfaceAddrs 在 windows 上可能返回 net.IPAddr 或 net.IPNet，在 linux 上则返回 net.IPNet。
			switch v := addr.(type) {
			case *net.IPAddr:
				ip = v.IP
			case *net.IPNet:
				ip = v.IP
			default:
				continue
			}

			if cidr.Contains(ip) {
				addresses[ip.String()] = ip
			}
		}
	}

	ips := make([]net.IP, 0, len(addresses))
	// 通过 map 转成切片会打乱顺序；调用方不应依赖返回列表的先后顺序。
	for _, ip := range addresses {
		ips = append(ips, ip)
	}

	return ips, nil
}

// ContainsIPv4Loopback 在 npa 的 CIDR 包含 IPv4 回环地址时返回 true。
func (npa *NodePortAddresses) ContainsIPv4Loopback() bool {
	return npa.containsIPv4Loopback
}

// ContainsExplicitLoopback 在 npa 的 CIDR 包含专门指向回环地址的 CIDR（例如 127.0.0.0/8 或 ::1/128）时返回 true，
// 与那些“恰好包含回环地址”的通配 CIDR 相区别。
func (npa *NodePortAddresses) ContainsExplicitLoopback() bool {
	return npa.containsExplicitLoopback
}
