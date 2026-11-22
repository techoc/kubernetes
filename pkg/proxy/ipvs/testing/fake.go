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

package testing

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/net"
)

// FakeNetlinkHandle 是 proxy NetlinkHandle 的模拟实现，
// 用内存 map 记录每张网卡的 IP 地址，便于在不操作真实内核网络栈的情况下跑测试。
type FakeNetlinkHandle struct {
	// localAddresses 记录网卡名称到其全部 IP 地址的映射，例如
	// eth0 -> [1.2.3.4, 10.20.30.40]；键存在即表示网卡已创建。
	localAddresses map[string][]string

	// IsIPv6 决定地址过滤时保留哪个地址族。
	IsIPv6 bool
}

// NewFakeNetlinkHandle 创建一个新的 FakeNetlinkHandle，isIPv6 指定其地址族。
func NewFakeNetlinkHandle(isIPv6 bool) *FakeNetlinkHandle {
	// 必须初始化 map，否则后续 EnsureAddressBind 的写入会 panic。
	fake := &FakeNetlinkHandle{
		localAddresses: make(map[string][]string),
		IsIPv6:         isIPv6,
	}
	return fake
}

// EnsureAddressBind 模拟地址绑定：地址已存在时返回 exist=true，
// 否则追加到对应网卡的地址列表，与真实 netlink 实现的行为保持一致。
func (h *FakeNetlinkHandle) EnsureAddressBind(address, devName string) (exist bool, err error) {
	// 空设备名在真实 netlink 中无意义，直接拒绝以免污染 mock 状态。
	if len(devName) == 0 {
		return false, fmt.Errorf("device name can't be empty")
	}
	// map 中不存在该键意味着网卡尚未创建，绑定到它应当失败。
	if _, ok := h.localAddresses[devName]; !ok {
		return false, fmt.Errorf("error bind address: %s to a non-exist interface: %s", address, devName)
	}
	// 先查重，避免同一地址被重复记入列表，保持与内核相同的幂等语义。
	for _, addr := range h.localAddresses[devName] {
		if addr == address {
			// 地址已绑定，返回 true 告知调用方无需再操作。
			return true, nil
		}
	}
	// 未绑定则将地址写入 mock 的网卡记录，供后续查询使用。
	h.localAddresses[devName] = append(h.localAddresses[devName], address)
	return false, nil
}

// UnbindAddress 模拟地址解绑：从网卡的地址列表中删除指定地址，
// 地址不存在时返回错误，便于测试覆盖异常路径。
func (h *FakeNetlinkHandle) UnbindAddress(address, devName string) error {
	// 空设备名无法定位任何网卡，直接返回错误。
	if len(devName) == 0 {
		return fmt.Errorf("device name can't be empty")
	}
	if _, ok := h.localAddresses[devName]; !ok {
		return fmt.Errorf("error unbind address: %s from a non-exist interface: %s", address, devName)
	}
	// 遍历切片找到目标地址的下标后再删除。
	for i, addr := range h.localAddresses[devName] {
		if addr == address {
			// 用切片拼接方式原地删除，并写回 map 以更新 mock 状态。
			h.localAddresses[devName] = append(h.localAddresses[devName][:i], h.localAddresses[devName][i+1:]...)
			return nil
		}
	}
	// 遍历完仍未找到，返回错误让测试能验证"解绑不存在的地址"的行为。
	return fmt.Errorf("address: %s is not found in interface: %s", address, devName)
}

// EnsureDummyDevice 模拟创建 dummy 网卡：以 map 中是否存在该键表示设备是否已创建，
// 已存在返回 true，否则新建一个空地址列表的条目并返回 false。
func (h *FakeNetlinkHandle) EnsureDummyDevice(devName string) (bool, error) {
	// 空设备名无法作为 map 键使用，直接拒绝。
	if len(devName) == 0 {
		return false, fmt.Errorf("device name can't be empty")
	}
	if _, ok := h.localAddresses[devName]; !ok {
		// 键不存在代表网卡未创建：用空切片占位，后续绑定地址时可直接使用。
		h.localAddresses[devName] = make([]string, 0)
		return false, nil
	}
	// 键已存在代表网卡已创建，返回 true 对应真实实现的幂等行为。
	return true, nil
}

// DeleteDummyDevice 模拟删除 dummy 网卡：从 map 中移除对应条目，
// 设备不存在时返回错误。
func (h *FakeNetlinkHandle) DeleteDummyDevice(devName string) error {
	// 空设备名无法定位网卡，直接返回错误。
	if len(devName) == 0 {
		return fmt.Errorf("device name can't be empty")
	}
	// 先检查存在性，避免把"删除不存在的设备"误报成成功。
	if _, ok := h.localAddresses[devName]; !ok {
		return fmt.Errorf("error deleting a non-exist interface: %s", devName)
	}
	// 删除整个键，同时清掉该网卡上记录的所有地址。
	delete(h.localAddresses, devName)
	return nil
}

// ListBindAddress 模拟列出指定网卡上绑定的所有地址，直接返回 mock 记录。
func (h *FakeNetlinkHandle) ListBindAddress(devName string) ([]string, error) {
	// 空设备名无法查询，直接返回错误。
	if len(devName) == 0 {
		return nil, fmt.Errorf("device name can't be empty")
	}
	// 不存在的网卡不能返回空结果，否则无法区分"无地址"和"无此网卡"。
	if _, ok := h.localAddresses[devName]; !ok {
		return nil, fmt.Errorf("error list addresses from a non-exist interface: %s", devName)
	}
	// 直接暴露内部切片，测试可通过结果断言地址列表内容。
	return h.localAddresses[devName], nil
}

// GetLocalAddresses 模拟查询指定网卡上的本地地址，
// 仅保留与句柄地址族一致、且非链路本地/回环的地址。
func (h *FakeNetlinkHandle) GetLocalAddresses(dev string) (sets.Set[string], error) {
	res := sets.New[string]()
	// 逐一检查该网卡记录的地址，模拟内核侧的地址族过滤。
	for _, addr := range h.localAddresses[dev] {
		if h.isValidForSet(addr) {
			res.Insert(addr)
		}
	}
	return res, nil
}

// GetAllLocalAddresses 模拟查询节点上所有网卡的本地地址，
// 与 GetLocalAddresses 使用同一过滤规则，仅地址来源范围不同。
func (h *FakeNetlinkHandle) GetAllLocalAddresses() (sets.Set[string], error) {
	res := sets.New[string]()
	// 遍历 map 的所有键，等价于遍历节点上所有网卡。
	for linkName := range h.localAddresses {
		// 对每张网卡逐个检查其记录的地址。
		for _, addr := range h.localAddresses[linkName] {
			if h.isValidForSet(addr) {
				res.Insert(addr)
			}
		}
	}
	return res, nil
}

// GetAllLocalAddressesExcept 模拟查询除指定网卡之外所有网卡的本地地址。
// 与从全量地址中减去单网卡地址不同，这里按网卡名排除，
// 以避免同一地址分配给多张网卡时被误删。
func (h *FakeNetlinkHandle) GetAllLocalAddressesExcept(dev string) (sets.Set[string], error) {
	res := sets.New[string]()
	for linkName := range h.localAddresses {
		// 按网卡名跳过目标设备，保留其他网卡上的同名地址。
		if linkName == dev {
			continue
		}
		// 仍按地址族和回环规则过滤，保持与其他查询方法一致。
		for _, addr := range h.localAddresses[linkName] {
			if h.isValidForSet(addr) {
				res.Insert(addr)
			}
		}
	}
	return res, nil
}

// SetLocalAddresses 为指定网卡设置 IP 地址列表，仅供测试准备 mock 状态，
// 不属于 NetlinkHandle 接口。会直接覆盖该网卡已有的地址。
func (h *FakeNetlinkHandle) SetLocalAddresses(dev string, ips ...string) error {
	// 允许零值结构体使用本方法，惰性初始化 map 防止写入 panic。
	if h.localAddresses == nil {
		h.localAddresses = make(map[string][]string)
	}
	// 空设备名无法作为 map 键，直接拒绝。
	if len(dev) == 0 {
		return fmt.Errorf("device name can't be empty")
	}
	// 每次调用都重建切片，确保旧地址不会残留。
	h.localAddresses[dev] = make([]string, 0)
	h.localAddresses[dev] = append(h.localAddresses[dev], ips...)
	return nil
}

// isValidForSet 判断字符串地址是否应出现在查询结果中：
// 地址族必须与句柄一致，并排除 IPv6 链路本地地址和回环地址。
func (h *FakeNetlinkHandle) isValidForSet(ipString string) bool {
	// 先解析成 net.IP；测试中传入的地址正常情况下均可解析。
	ip := net.ParseIPSloppy(ipString)
	// To4() == nil 表示 IPv6 地址，与句柄的 IsIPv6 必须一致。
	if h.IsIPv6 != (ip.To4() == nil) {
		return false
	}
	if h.IsIPv6 && ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.IsLoopback() {
		return false
	}
	return true
}
