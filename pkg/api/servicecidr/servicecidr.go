/*
Copyright 2024 The Kubernetes Authors.

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

package servicecidr

import (
	"fmt"
	"net"
	"net/netip"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
)

// OverlapsPrefix 返回所有与给定 prefix 有重叠（哪怕只是部分相交）的 ServiceCIDR 列表。
func OverlapsPrefix(serviceCIDRLister networkinglisters.ServiceCIDRLister, prefix netip.Prefix) []*networkingv1.ServiceCIDR {
	result := []*networkingv1.ServiceCIDR{}
	// List 失败时返回空列表：调用方只需知道“没有匹配的 CIDR”。
	serviceCIDRList, err := serviceCIDRLister.List(labels.Everything())
	if err != nil {
		return result
	}

	for _, serviceCIDR := range serviceCIDRList {
		for _, cidr := range serviceCIDR.Spec.CIDRs {
			// CIDR 已经过 API 校验，这里解析不可能失败。
			if p, err := netip.ParsePrefix(cidr); err == nil {
				if p.Overlaps(prefix) {
					result = append(result, serviceCIDR)
				}
			}
		}
	}
	return result
}

// ContainsPrefix 返回所有完整包含给定 prefix 的 ServiceCIDR 列表。
func ContainsPrefix(serviceCIDRLister networkinglisters.ServiceCIDRLister, prefix netip.Prefix) []*networkingv1.ServiceCIDR {
	result := []*networkingv1.ServiceCIDR{}
	serviceCIDRList, err := serviceCIDRLister.List(labels.Everything())
	if err != nil {
		return result
	}

	for _, serviceCIDR := range serviceCIDRList {
		for _, cidr := range serviceCIDR.Spec.CIDRs {
			if p, err := netip.ParsePrefix(cidr); err == nil { // CIDR 已校验，解析不会失败
				// 除了重叠，还要求 ServiceCIDR 的前缀长度更短（范围更大）。
				if p.Overlaps(prefix) && p.Bits() <= prefix.Bits() {
					result = append(result, serviceCIDR)
				}
			}
		}
	}
	return result
}

// ContainsIP 返回所有包含给定 IP 地址的 ServiceCIDR 列表。
func ContainsIP(serviceCIDRLister networkinglisters.ServiceCIDRLister, ip net.IP) []*networkingv1.ServiceCIDR {
	// 先做类型归一化，再委托给 ContainsAddress。
	address := IPToAddr(ip)
	return ContainsAddress(serviceCIDRLister, address)
}

// ContainsAddress 返回所有包含给定 netip.Addr 的 ServiceCIDR 列表。
func ContainsAddress(serviceCIDRLister networkinglisters.ServiceCIDRLister, address netip.Addr) []*networkingv1.ServiceCIDR {
	result := []*networkingv1.ServiceCIDR{}
	serviceCIDRList, err := serviceCIDRLister.List(labels.Everything())
	if err != nil {
		return result
	}

	for _, serviceCIDR := range serviceCIDRList {
		for _, cidr := range serviceCIDR.Spec.CIDRs {
			if prefix, err := netip.ParsePrefix(cidr); err == nil { // CIDR 已校验，解析不会失败
				if PrefixContainsIP(prefix, address) {
					result = append(result, serviceCIDR)
				}
			}
		}
	}
	return result
}

// PrefixContainsIP 判断给定 IP 是否被 prefix 包含，同时排除网络地址和（IPv4 的）广播地址。
// 不能简单地用 prefix.Contains(ip)：ServiceCIDR 不会分配这两种地址，
// 因此拥有这些 IP 的 Service 不属于该 ServiceCIDR。
func PrefixContainsIP(prefix netip.Prefix, ip netip.Addr) bool {
	// 网络地址不视为包含。
	if prefix.Masked().Addr() == ip {
		return false
	}
	// IPv4 还要排除广播地址。
	if ip.Is4() {
		ipLast, err := broadcastAddress(prefix)
		if err != nil || ipLast == ip {
			return false
		}
	}
	return prefix.Contains(ip)
}

// broadcastAddress 计算子网广播地址：把子网内所有主机位都置为 1。
// 例如 192.168.0.0/24：子网位 24，主机位 32-24=8，广播地址为 192.168.0.255。
func broadcastAddress(subnet netip.Prefix) (netip.Addr, error) {
	// Masked() 消除主机位上的杂散 bit，得到规范的网络地址。
	base := subnet.Masked().Addr()
	bytes := base.AsSlice()
	// n 是需要置 1 的主机位数量。
	n := 8*len(bytes) - subnet.Bits()
	// 从最后一个字节开始向前处理，整字节置 0xff，剩余位用掩码置 1。
	for i := len(bytes) - 1; i >= 0 && n > 0; i-- {
		if n >= 8 {
			bytes[i] = 0xff
			n -= 8
		} else {
			mask := ^uint8(0) >> (8 - n)
			bytes[i] |= mask
			break
		}
	}

	addr, ok := netip.AddrFromSlice(bytes)
	if !ok {
		return netip.Addr{}, fmt.Errorf("invalid address %v", bytes)
	}
	return addr, nil
}

// IPToAddr 把 net.IP 转换为 netip.Addr；输入无效时返回空的 netip.Addr{}。
func IPToAddr(ip net.IP) netip.Addr {
	// netip.AddrFromSlice 可能把 IPv4 地址表示成 IPv4-mapped IPv6 形式
	// （例如 ::ffff:192.168.0.1），所以要先用 To4/To16 明确地址族，
	// 保证返回的地址格式与期望一致。
	bytes := ip.To4()
	if bytes == nil {
		bytes = ip.To16()
	}
	// AddrFromSlice 对无效输入返回 Addr{}, false，这里直接取零值即可。
	address, _ := netip.AddrFromSlice(bytes)
	return address
}
