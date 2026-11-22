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
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	netutils "k8s.io/utils/net"
)

// 代理规则同步中常用的常量。
const (
	// IPv4ZeroCIDR 表示整个 IPv4 地址空间（全零掩码的网段）。
	IPv4ZeroCIDR = "0.0.0.0/0"

	// IPv6ZeroCIDR 表示整个 IPv6 地址空间。
	IPv6ZeroCIDR = "::/0"

	// FullSyncPeriod 是 iptables 与 nftables 代理执行全量同步的周期。
	FullSyncPeriod = 1 * time.Hour
)

// IsZeroCIDR 判断给定 CIDR 是否为 IPv4 或 IPv6 的零 CIDR（掩码长度为 0）。
func IsZeroCIDR(cidr *net.IPNet) bool {
	if cidr == nil {
		return false
	}
	prefixLen, _ := cidr.Mask.Size()
	return prefixLen == 0
}

// ShouldSkipService 判断给定 Service 是否应跳过代理。
func ShouldSkipService(service *v1.Service) bool {
	// headless 服务（ClusterIP 为 None）或无 ClusterIP 的服务无需代理。
	if !helper.IsServiceIPSet(service) {
		klog.V(3).InfoS("Skipping service due to cluster IP", "service", klog.KObj(service), "clusterIP", service.Spec.ClusterIP)
		return true
	}
	// ExternalName 服务由 DNS 解析处理，即使设置了 ClusterIP 也不建立代理规则。
	if service.Spec.Type == v1.ServiceTypeExternalName {
		klog.V(3).InfoS("Skipping service due to Type=ExternalName", "service", klog.KObj(service))
		return true
	}
	return false
}

// AddressSet 用 isValid 过滤地址切片，并把通过校验的地址以字符串集合返回。
func AddressSet(isValid func(ip net.IP) bool, addrs []net.Addr) sets.Set[string] {
	ips := sets.New[string]()
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPAddr:
			ip = v.IP
		case *net.IPNet:
			ip = v.IP
		default:
			// 非 IP 地址类型直接跳过。
			continue
		}
		if isValid(ip) {
			ips.Insert(ip.String())
		}
	}
	return ips
}

// MapIPsByIPFamily 将 IP 字符串按所属 IP 族（IPv4/IPv6）分组。
func MapIPsByIPFamily(ipStrings []string) map[v1.IPFamily][]net.IP {
	ipFamilyMap := map[v1.IPFamily][]net.IP{}
	for _, ipStr := range ipStrings {
		ip := netutils.ParseIPSloppy(ipStr)
		if ip != nil {
			// 解析成功意味着 IP 族必定已知。
			ipFamily := GetIPFamilyFromIP(ip)
			ipFamilyMap[ipFamily] = append(ipFamilyMap[ipFamily], ip)
		} else {
			// ExternalIP 可能未经 API Server 校验，尤其空字符串很常见；
			// 为其打印错误日志会产生大量噪音，因此静默忽略。
			if len(strings.TrimSpace(ipStr)) != 0 {
				klog.ErrorS(nil, "Skipping invalid IP", "ip", ipStr)
			}
		}
	}
	return ipFamilyMap
}

// MapCIDRsByIPFamily 将 CIDR 字符串按所属 IP 族（IPv4/IPv6）分组。
func MapCIDRsByIPFamily(cidrsStrings []string) map[v1.IPFamily][]*net.IPNet {
	ipFamilyMap := map[v1.IPFamily][]*net.IPNet{}
	for _, cidrStrUntrimmed := range cidrsStrings {
		cidrStr := strings.TrimSpace(cidrStrUntrimmed)
		_, cidr, err := netutils.ParseCIDRSloppy(cidrStr)
		if err != nil {
			// 与 MapIPsByIPFamily 一致，空字符串无需记录错误日志。
			if len(cidrStr) != 0 {
				klog.ErrorS(err, "Invalid CIDR ignored", "CIDR", cidrStr)
			}
			continue
		}
		// 刚成功解析过，IP 族不可能是未知值。
		ipFamily := convertToV1IPFamily(netutils.IPFamilyOfCIDR(cidr))
		ipFamilyMap[ipFamily] = append(ipFamilyMap[ipFamily], cidr)
	}
	return ipFamilyMap
}

// GetIPFamilyFromIP 返回 IP 所属的族；无法识别时返回 IPFamilyUnknown。
func GetIPFamilyFromIP(ip net.IP) v1.IPFamily {
	return convertToV1IPFamily(netutils.IPFamilyOf(ip))
}

// convertToV1IPFamily 把 netutils 的 IP 族类型转换为 API 的 v1.IPFamily。
func convertToV1IPFamily(ipFamily netutils.IPFamily) v1.IPFamily {
	switch ipFamily {
	case netutils.IPv4:
		return v1.IPv4Protocol
	case netutils.IPv6:
		return v1.IPv6Protocol
	}

	return v1.IPFamilyUnknown
}

// OtherIPFamily 返回与给定 IP 族相对的另一种族（IPv4 与 IPv6 互换）。
func OtherIPFamily(ipFamily v1.IPFamily) v1.IPFamily {
	if ipFamily == v1.IPv6Protocol {
		return v1.IPv4Protocol
	}

	return v1.IPv6Protocol
}

// AppendPortIfNeeded 在地址尚未包含端口时追加端口，保留已有的 "ipv4:port" 或 "[ipv6]:port" 格式。
func AppendPortIfNeeded(addr string, port int32) string {
	// 已带端口的地址直接返回，避免重复追加。
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}

	// 非法地址应由前置校验拦截；这里原样返回，避免生成无意义的规则。
	ip := netutils.ParseIPSloppy(addr)
	if ip == nil {
		return addr
	}

	// JoinHostPort 会自动为 IPv6 地址加上方括号。
	return net.JoinHostPort(addr, strconv.Itoa(int(port)))
}

// EnsureSysctl 在内核 sysctl 当前值与目标值不同时进行设置，避免不必要的写入。
func EnsureSysctl(sysctl utilsysctl.Interface, name string, newVal int) error {
	if oldVal, _ := sysctl.GetSysctl(name); oldVal != newVal {
		if err := sysctl.SetSysctl(name, newVal); err != nil {
			return fmt.Errorf("can't set sysctl %s to %d: %v", name, newVal, err)
		}
		klog.V(1).InfoS("Changed sysctl", "name", name, "before", oldVal, "after", newVal)
	}
	return nil
}

// GetClusterIPByFamily 返回 Service 在指定 IP 族上的 ClusterIP。
func GetClusterIPByFamily(ipFamily v1.IPFamily, service *v1.Service) string {
	// 兼容版本偏差：旧版本 Service 可能尚未填写 IPFamilies，需回退到 ClusterIP 字段推断。
	if len(service.Spec.IPFamilies) == 0 {
		if len(service.Spec.ClusterIP) == 0 || service.Spec.ClusterIP == v1.ClusterIPNone {
			return ""
		}

		IsIPv6Family := (ipFamily == v1.IPv6Protocol)
		if IsIPv6Family == netutils.IsIPv6String(service.Spec.ClusterIP) {
			return service.Spec.ClusterIP
		}

		return ""
	}

	for idx, family := range service.Spec.IPFamilies {
		if family == ipFamily {
			if idx < len(service.Spec.ClusterIPs) {
				return service.Spec.ClusterIPs[idx]
			}
		}
	}

	return ""
}

// IsVIPMode 判断负载均衡入口是否使用 VIP 模式；未显式设置 IPMode 时按 VIP 处理。
func IsVIPMode(ing v1.LoadBalancerIngress) bool {
	if ing.IPMode == nil {
		return true
	}
	return *ing.IPMode == v1.LoadBalancerIPModeVIP
}
