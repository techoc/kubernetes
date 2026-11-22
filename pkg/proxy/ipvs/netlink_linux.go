//go:build linux

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

package ipvs

import (
	"fmt"
	"net"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	netutils "k8s.io/utils/net"
)

// netlinkHandle 在 netlink.Handle 之上记录地址族信息，
// 便于按 IPv4/IPv6 过滤本地地址。
type netlinkHandle struct {
	netlink.Handle
	isIPv6 bool
}

// NewNetLinkHandle 创建一个新的 NetLinkHandle，isIPv6 指定其地址族。
func NewNetLinkHandle(isIPv6 bool) NetLinkHandle {
	return &netlinkHandle{netlink.Handle{}, isIPv6}
}

// EnsureAddressBind 检查地址是否已绑定到网卡，未绑定则执行绑定。
// 地址已存在时返回 exist=true，让调用方无需重复下发 netlink 请求。
func (h *netlinkHandle) EnsureAddressBind(address, devName string) (exist bool, err error) {
	// AddrAdd 需要 link 对象而非名称，先按名称查到设备。
	dev, err := h.LinkByName(devName)
	if err != nil {
		return false, fmt.Errorf("error get interface: %s, err: %v", devName, err)
	}
	addr := netutils.ParseIPSloppy(address)
	if addr == nil {
		return false, fmt.Errorf("error parse ip address: %s", address)
	}
	// 下发地址绑定请求；重复绑定会失败，需要用 EEXIST 判断幂等成功。
	if err := h.AddrAdd(dev, &netlink.Addr{IPNet: netlink.NewIPNet(addr)}); err != nil {
		// 内核对已绑定地址返回 EEXIST：这不是错误，而是"地址已存在"的信号。
		if err == unix.EEXIST {
			return true, nil
		}
		return false, fmt.Errorf("error bind address: %s to interface: %s, err: %v", address, devName, err)
	}
	return false, nil
}

// UnbindAddress 确保 IP 地址已从网卡解绑；地址本就不存在时也返回成功，保证幂等。
func (h *netlinkHandle) UnbindAddress(address, devName string) error {
	// AddrDel 需要 link 对象，因此先按名称找到设备。
	dev, err := h.LinkByName(devName)
	if err != nil {
		return fmt.Errorf("error get interface: %s, err: %v", devName, err)
	}
	addr := netutils.ParseIPSloppy(address)
	if addr == nil {
		return fmt.Errorf("error parse ip address: %s", address)
	}
	// 下发解绑请求；内核用 ENXIO 表示地址不存在，应视为已经达成目标。
	if err := h.AddrDel(dev, &netlink.Addr{IPNet: netlink.NewIPNet(addr)}); err != nil {
		// ENXIO（无此设备/地址）意味着地址已不在该网卡上，无需报错。
		if err != unix.ENXIO {
			return fmt.Errorf("error unbind address: %s from interface: %s, err: %v", address, devName, err)
		}
	}
	return nil
}

// EnsureDummyDevice 确保 dummy 网卡存在：已存在返回 true，否则创建并返回 false。
func (h *netlinkHandle) EnsureDummyDevice(devName string) (bool, error) {
	// 先查询，避免重复创建导致 Add 失败。
	_, err := h.LinkByName(devName)
	if err == nil {
		// 网卡已存在，调用方无需再创建。
		return true, nil
	}
	// 查询失败则按给定名称构造 dummy 设备并下发创建请求。
	dummy := &netlink.Dummy{
		LinkAttrs: netlink.LinkAttrs{Name: devName},
	}
	return false, h.LinkAdd(dummy)
}

// DeleteDummyDevice 删除 dummy 网卡；设备不存在时静默成功，
// 但设备存在却不是 dummy 类型时报错，防止误删其他网卡。
func (h *netlinkHandle) DeleteDummyDevice(devName string) error {
	// 先取出设备对象，删除接口需要具体的 link，而不是名称。
	link, err := h.LinkByName(devName)
	if err != nil {
		// 找不到设备说明已达成目标，删除操作应当幂等。
		_, ok := err.(netlink.LinkNotFoundError)
		if ok {
			return nil
		}
		return fmt.Errorf("error deleting a non-exist dummy device: %s, %v", devName, err)
	}
	// 类型断言确保只删除 dummy 设备。
	dummy, ok := link.(*netlink.Dummy)
	if !ok {
		return fmt.Errorf("expect dummy device, got device type: %s", link.Type())
	}
	return h.LinkDel(dummy)
}

// ListBindAddress 列出指定网卡上绑定的所有 IP 地址。
func (h *netlinkHandle) ListBindAddress(devName string) ([]string, error) {
	// AddrList 以 link 为过滤条件，先解析设备名。
	dev, err := h.LinkByName(devName)
	if err != nil {
		return nil, fmt.Errorf("error get interface: %s, err: %v", devName, err)
	}
	// 0 表示不按地址族过滤，拿到该网卡的全部地址后再由调用方处理。
	addrs, err := h.AddrList(dev, 0)
	if err != nil {
		return nil, fmt.Errorf("error list bound address of interface: %s, err: %v", devName, err)
	}
	var ips []string
	for _, addr := range addrs {
		ips = append(ips, addr.IP.String())
	}
	return ips, nil
}

// GetAllLocalAddresses 返回节点上的所有本地地址。
// 只返回与当前句柄地址族一致的地址。
// IPv6 链路本地地址和回环地址不计入结果。
func (h *netlinkHandle) GetAllLocalAddresses() (sets.Set[string], error) {
	// 从标准库一次性取全部地址，地址族与业务无关的过滤交给 isValidForSet。
	addr, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("Could not get addresses: %v", err)
	}
	return proxyutil.AddressSet(h.isValidForSet, addr), nil
}

// GetLocalAddresses 返回指定网卡上的所有本地地址。
// 只返回与当前句柄地址族一致的地址。
// IPv6 链路本地地址和回环地址不计入结果。
func (h *netlinkHandle) GetLocalAddresses(dev string) (sets.Set[string], error) {
	// 先把网卡名解析成接口对象，再读取其地址列表。
	ifi, err := net.InterfaceByName(dev)
	if err != nil {
		return nil, fmt.Errorf("Could not get interface %s: %v", dev, err)
	}
	addr, err := ifi.Addrs()
	if err != nil {
		return nil, fmt.Errorf("Can't get addresses from %s: %v", ifi.Name, err)
	}
	return proxyutil.AddressSet(h.isValidForSet, addr), nil
}

// isValidForSet 判断地址是否应加入本地地址集合：地址族必须与句柄一致，
// 并排除 IPv6 链路本地地址以及所有回环地址。
func (h *netlinkHandle) isValidForSet(ip net.IP) bool {
	if h.isIPv6 != netutils.IsIPv6(ip) {
		return false
	}
	if h.isIPv6 && ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.IsLoopback() {
		return false
	}
	return true
}

// GetAllLocalAddressesExcept 返回节点上除指定网卡之外的所有本地地址。
// 这不能简单地用 GetAllLocalAddresses 与 GetLocalAddresses 做差集得到，
// 因为同一个地址可能同时分配给多张网卡；相关背景见
// https://github.com/kubernetes/kubernetes/issues/114815
func (h *netlinkHandle) GetAllLocalAddressesExcept(dev string) (sets.Set[string], error) {
	// 旧实现遍历 net.Interfaces() 并对每张网卡调用 iface.Addrs()，但 iface.Addrs()
	// 内部会对整个节点做一次完整的 RTM_GETADDR netlink 转储，再在用户态过滤。
	// 当网卡和地址都很多时（例如 kube-ipvs0 上绑定了数万个 ClusterIP），
	// 复杂度为 O(N_interfaces * N_addresses)，会成为 syncProxyRules 延迟的主要来源。
	//
	// 现在改为用一次 AF_UNSPEC 的 RTM_GETADDR 调用转储节点全部地址，
	// 再按 LinkIndex 跳过目标网卡的地址，复杂度降为 O(N_addresses)，
	// 避免按网卡逐个发起请求。
	devLink, err := netlink.LinkByName(dev)
	if err != nil {
		klog.ErrorS(err, "Could not look up link", "dev", dev)
		return nil, fmt.Errorf("could not look up link %q: %w", dev, err)
	}
	// 记录目标网卡的内核索引，用于从转储结果中排除其地址。
	devIndex := devLink.Attrs().Index

	// nil 网卡参数表示转储整个节点的地址，AF_UNSPEC 同时覆盖 IPv4 与 IPv6。
	addrs, err := netlink.AddrList(nil, unix.AF_UNSPEC)
	if err != nil {
		klog.ErrorS(err, "Failed to dump node addresses")
		return nil, fmt.Errorf("could not list node addresses: %w", err)
	}

	// 过滤后统一交给 isValidForSet 剔除非本地址族、链路本地和回环地址。
	return proxyutil.AddressSet(h.isValidForSet, filterAddrsExcept(addrs, devIndex)), nil
}

// filterAddrsExcept 返回 LinkIndex 不等于 devIndex 的地址。
// 单独抽成函数是为了无需 root 权限和真实网络命名空间即可做单元测试。
func filterAddrsExcept(addrs []netlink.Addr, devIndex int) []net.Addr {
	out := make([]net.Addr, 0, len(addrs))
	for _, a := range addrs {
		// 属于目标网卡的地址直接跳过。
		if a.LinkIndex == devIndex {
			continue
		}
		// 没有 IPNet 的地址无法转成 net.Addr，忽略以防空指针。
		if a.IPNet == nil {
			continue
		}
		out = append(out, a.IPNet)
	}
	return out
}
