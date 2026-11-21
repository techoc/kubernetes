//go:build windows

/*
Copyright 2023 The Kubernetes Authors.

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
	"os"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

const (
	// Windows 注册表路径与 DNS 字段的最大长度限制。
	netRegistry       = `System\CurrentControlSet\Services\TCPIP\Parameters`
	netIfacesRegistry = `System\CurrentControlSet\Services\TCPIP\Parameters\Interfaces`
	maxHostnameLen    = 128
	maxDomainNameLen  = 128
	maxScopeIDLen     = 256

	// resolverConfig 为 "Host" 时，getHostDNSConfig 返回宿主机的 DNS 配置。
	hostResolvConf = "Host"
)

// FixedInfo 对应 Windows 的 FIXED_INFO 结构。
// https://docs.microsoft.com/en-us/windows/win32/api/iptypes/ns-iptypes-fixed_info_w2ksp1
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
	// GetNetworkParams 位于 iphlpapi.dll。
	// 参见：https://docs.microsoft.com/windows/win32/api/iphlpapi/nf-iphlpapi-getnetworkparams
	iphlpapidll          = windows.MustLoadDLL("iphlpapi.dll")
	procGetNetworkParams = iphlpapidll.MustFindProc("GetNetworkParams")
)

// fileExists 判断路径是否存在且为普通文件。
func fileExists(filename string) (bool, error) {
	stat, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return stat.Mode().IsRegular(), nil
}

// getHostDNSConfig 根据 resolverConfig 返回 DNS 配置：空值使用默认配置，
// 文件路径读取文件，"Host" 读取宿主机设置。
func getHostDNSConfig(logger klog.Logger, resolverConfig string) (*runtimeapi.DNSConfig, error) {
	if resolverConfig == "" {
		// 空值表示使用默认配置。
		return getDNSConfig(logger, resolverConfig)
	}

	isFile, err := fileExists(resolverConfig)
	if err != nil {
		err = fmt.Errorf(`Unexpected error while getting os.Stat for "%s" resolver config. Error: %w`, resolverConfig, err)
		logger.Error(err, "Cannot get host DNS Configuration.")
		return nil, err
	}
	if isFile {
		// 从类似 resolv.conf 的文件读取 DNS 配置。
		return getDNSConfig(logger, resolverConfig)
	}

	if resolverConfig != hostResolvConf {
		err := fmt.Errorf(`Unexpected resolver config value: "%s". Expected "", "%s", or a path to an existing resolv.conf file.`, resolverConfig, hostResolvConf)
		logger.Error(err, "Cannot get host DNS Configuration.")
		return nil, err
	}

	// 走到这里说明 resolverConfig 等于 hostResolvConf 且它不是实际文件，
	// 因此使用宿主机的 DNS 设置。
	hostDNS, err := getDNSServerList()
	if err != nil {
		err = fmt.Errorf("Could not get the host's DNS Server List. Error: %w", err)
		logger.Error(err, "Encountered error while getting host's DNS Server List.")
		return nil, err
	}
	hostSearch, err := getDNSSuffixList()
	if err != nil {
		err = fmt.Errorf("Could not get the host's DNS Suffix List. Error: %w", err)
		logger.Error(err, "Encountered error while getting host's DNS Suffix List.")
		return nil, err
	}
	return &runtimeapi.DNSConfig{
		Servers:  hostDNS,
		Searches: hostSearch,
	}, nil
}

// elemInList 判断元素是否存在于字符串列表中。
func elemInList(elem string, list []string) bool {
	for _, e := range list {
		if e == elem {
			return true
		}
	}
	return false
}

// getRegistryStringValue 读取注册表指定路径下的字符串值。
func getRegistryStringValue(reg, key string) (string, error) {
	regKey, err := registry.OpenKey(registry.LOCAL_MACHINE, reg, registry.QUERY_VALUE)
	if err != nil {
		return "", err
	}
	defer regKey.Close()

	regValue, _, err := regKey.GetStringValue(key)
	return regValue, err
}

// getDNSSuffixList 读取 DNS 配置并返回已配置的 DNS 后缀列表。
func getDNSSuffixList() ([]string, error) {
	// 先收集适用于所有网络连接的全局后缀列表。
	allSuffixes := []string{}
	suffixes, err := getRegistryStringValue(netRegistry, "SearchList")
	if err != nil {
		return nil, err
	}
	allSuffixes = strings.Split(suffixes, ",")

	// 再追加各网络接口专属的 DNS 后缀列表。
	regKey, err := registry.OpenKey(registry.LOCAL_MACHINE, netIfacesRegistry, registry.ENUMERATE_SUB_KEYS)
	if err != nil {
		return nil, err
	}
	defer regKey.Close()

	ifaces, err := regKey.ReadSubKeyNames(0)
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		suffixes, err := getRegistryStringValue(fmt.Sprintf("%s\\%s", netIfacesRegistry, iface), "SearchList")
		if err != nil {
			return nil, err
		}
		if suffixes == "" {
			continue
		}
		// 去重后追加接口级后缀。
		for _, suffix := range strings.Split(suffixes, ",") {
			if !elemInList(suffix, allSuffixes) {
				allSuffixes = append(allSuffixes, suffix)
			}
		}
	}

	return allSuffixes, nil
}

// getNetworkParams 调用 GetNetworkParams 获取 Windows 网络参数。
func getNetworkParams() (*FixedInfo, error) {
	// 先传入极小缓冲区，调用会通过 size 返回所需的实际大小。
	var size int = 1
	buffer := make([]byte, 1)
	ret, _, err := procGetNetworkParams.Call(
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&size)),
	)
	// 第一次调用预期返回 ERROR_BUFFER_OVERFLOW，并给出所需大小。
	if ret != uintptr(syscall.ERROR_BUFFER_OVERFLOW) {
		err = fmt.Errorf("Unexpected return value %d from GetNetworkParams. Expected: %d. Error: %w", ret, syscall.ERROR_BUFFER_OVERFLOW, err)
		return nil, err
	}

	// 按所需大小重新分配缓冲区并再次调用。
	buffer = make([]byte, size)
	ret, _, err = procGetNetworkParams.Call(
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&size)),
	)
	if ret != 0 {
		err = fmt.Errorf("Unexpected return value %d from GetNetworkParams. Expected: 0. Error: %w", ret, err)
		return nil, err
	}

	info := (*FixedInfo)(unsafe.Pointer(&buffer[0]))
	return info, nil
}

// getDNSServerList 返回宿主机配置的 DNS 服务器列表。
func getDNSServerList() ([]string, error) {
	dnsServerList := []string{}
	fixedInfo, err := getNetworkParams()
	if err != nil {
		return nil, err
	}

	list := &(fixedInfo.DNSServerList)
	// 沿链表遍历所有 DNS 服务器，并去除字符串末尾的 NUL 字节。
	for list != nil {
		dnsServer := strings.TrimRight(string(list.IpAddress.String[:]), "\x00")
		dnsServerList = append(dnsServerList, dnsServer)
		list = list.Next
	}
	return dnsServerList, nil
}
