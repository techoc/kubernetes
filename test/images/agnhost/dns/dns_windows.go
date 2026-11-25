//go:build windows

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

package dns

import (
	"fmt"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

const (
	etcHostsFile      = "C:/Windows/System32/drivers/etc/hosts"
	netRegistry       = `System\CurrentControlSet\Services\TCPIP\Parameters`
	netIfacesRegistry = `System\CurrentControlSet\Services\TCPIP\Parameters\Interfaces`
	maxHostnameLen    = 128
	maxDomainNameLen  = 128
	maxScopeIDLen     = 256
)

// FixedInfo 信息：https://docs.microsoft.com/en-us/windows/win32/api/iptypes/ns-iptypes-fixed_info_w2ksp1
type FixedInfo struct {
	HostName         [maxHostnameLen + 4]byte
	DomainName       [maxDomainNameLen + 4]byte
	CurrentDNSServer *syscall.IpAddrString
	DNSServerList    syscall.IpAddrString
	NodeType         uint32
	ScopeID          [maxScopeIDLen + 4]byte
	EnableRouting    uint32
	EnableProxy      uint32
	EnableDNS        uint32
}

var (
	// GetNetworkParams 位于 iphlpapi.dll 中
	// 参见：https://docs.microsoft.com/en-us/windows/win32/api/iphlpapi/nf-iphlpapi-getnetworkparams?redirectedfrom=MSDN
	iphlpapidll          = windows.MustLoadDLL("iphlpapi.dll")
	procGetNetworkParams = iphlpapidll.MustFindProc("GetNetworkParams")
)

func elemInList(elem string, list []string) bool {
	for _, e := range list {
		if e == elem {
			return true
		}
	}
	return false
}

func getRegistryValue(reg, key string) string {
	regKey, err := registry.OpenKey(registry.LOCAL_MACHINE, reg, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer regKey.Close()

	regValue, _, err := regKey.GetStringValue(key)
	if err != nil {
		return ""
	}
	return regValue
}

// GetDNSSuffixList 读取 DNS 配置文件，返回已配置的 DNS 后缀列表
func GetDNSSuffixList() []string {
	// 首先收集适用于所有网络连接的通用后缀列表。
	allSuffixes := []string{}
	suffixes := getRegistryValue(netRegistry, "SearchList")
	if suffixes != "" {
		allSuffixes = strings.Split(suffixes, ",")
	}

	// 然后追加特定于网络的 DNS 后缀列表。
	regKey, err := registry.OpenKey(registry.LOCAL_MACHINE, netIfacesRegistry, registry.ENUMERATE_SUB_KEYS)
	if err != nil {
		panic(err)
	}
	defer regKey.Close()

	ifaces, err := regKey.ReadSubKeyNames(0)
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		suffixes := getRegistryValue(fmt.Sprintf("%s\\%s", netIfacesRegistry, iface), "SearchList")
		if suffixes == "" {
			continue
		}
		for _, suffix := range strings.Split(suffixes, ",") {
			if !elemInList(suffix, allSuffixes) {
				allSuffixes = append(allSuffixes, suffix)
			}
		}
	}

	return allSuffixes
}

func getNetworkParams() *FixedInfo {
	// 我们不知道字节缓冲区应该设多大，但该调用会在之后
	// 通过设置 size 告知我们。
	var size int
	buffer := make([]byte, 1)
	procGetNetworkParams.Call(
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&size)),
	)

	buffer = make([]byte, size)
	procGetNetworkParams.Call(
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&size)),
	)

	info := (*FixedInfo)(unsafe.Pointer(&buffer[0]))
	return info
}

func getDNSServerList() []string {
	dnsServerList := []string{}
	fixedInfo := getNetworkParams()
	list := &(fixedInfo.DNSServerList)

	for list != nil {
		dnsServer := strings.TrimRight(string(list.IpAddress.String[:]), "\x00")
		dnsServerList = append(dnsServerList, dnsServer)
		list = list.Next
	}
	return dnsServerList
}
