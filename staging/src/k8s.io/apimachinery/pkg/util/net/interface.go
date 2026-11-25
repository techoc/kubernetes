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

// Package net 提供主机网络接口与路由的探测工具，用于为守护进程选择
// 合适的绑定地址（优先 IPv4），并屏蔽测试对真实 /proc 与 net 库的依赖。
package net

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"

	"strings"

	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// AddressFamily 是 IP 地址族（4 或 6），值本身即协议版本号。
type AddressFamily uint

const (
	familyIPv4 AddressFamily = 4
	familyIPv6 AddressFamily = 6
)

// AddressFamilyPreference 是按优先顺序排列的地址族列表；调用方按顺序尝试，
// 因此 "preferIPv4" 表示 IPv4 优先。
type AddressFamilyPreference []AddressFamily

var (
	preferIPv4 = AddressFamilyPreference{familyIPv4, familyIPv6}
	preferIPv6 = AddressFamilyPreference{familyIPv6, familyIPv4}
)

const (
	//LoopbackInterfaceName 是回环接口的默认名称。
	LoopbackInterfaceName = "lo"
)

const (
	// Linux 的 /proc 路由表格式；这里只解析默认路由。
	ipv4RouteFile = "/proc/net/route"
	ipv6RouteFile = "/proc/net/ipv6_route"
)

// Route 是路由表中的一条默认路由。
type Route struct {
	Interface   string
	Destination net.IP
	Gateway     net.IP
	Family      AddressFamily
}

// RouteFile 描述一个路由文件及其解析函数；把路径与解析逻辑分开便于测试。
type RouteFile struct {
	name  string
	parse func(input io.Reader) ([]Route, error)
}

// 没有路由时可能返回 noRoutesError。
type noRoutesError struct {
	message string
}

func (e noRoutesError) Error() string {
	return e.message
}

// IsNoRoutesError 检查错误是否为 noRoutesError 类型。
func IsNoRoutesError(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case noRoutesError:
		return true
	default:
		return false
	}
}

var (
	v4File = RouteFile{name: ipv4RouteFile, parse: getIPv4DefaultRoutes}
	v6File = RouteFile{name: ipv6RouteFile, parse: getIPv6DefaultRoutes}
)

// extract 打开路由文件并把内容交给解析函数。调用方需区分“文件不存在”
// （走接口探测兜底）与“解析失败”（直接报错）。
func (rf RouteFile) extract() ([]Route, error) {
	file, err := os.Open(rf.name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return rf.parse(file)
}

// getIPv4DefaultRoutes 获取 IPv4 路由并过滤非默认路由。
func getIPv4DefaultRoutes(input io.Reader) ([]Route, error) {
	routes := []Route{}
	scanner := bufio.NewReader(input)
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			break
		}
		//忽略路由信息中的头部字段
		if strings.HasPrefix(line, "Iface") {
			continue
		}
		fields := strings.Fields(line)
		//关注以下字段：
		//0 - 接口名称
		//1 - 目标地址
		//2 - 网关
		dest, err := parseIP(fields[1], familyIPv4)
		if err != nil {
			return nil, err
		}
		gw, err := parseIP(fields[2], familyIPv4)
		if err != nil {
			return nil, err
		}
		// 只保留默认路由（目的地址为 0.0.0.0）。
		if !dest.Equal(net.IPv4zero) {
			continue
		}
		routes = append(routes, Route{
			Interface:   fields[0],
			Destination: dest,
			Gateway:     gw,
			Family:      familyIPv4,
		})
	}
	return routes, nil
}

// getIPv6DefaultRoutes 解析 /proc/net/ipv6_route，列位置与 IPv4 不同：
// 目的地址、网关、接口名分别在第 0、4、9 列。网关为零的条目是 loopback，
// 需要排除。
func getIPv6DefaultRoutes(input io.Reader) ([]Route, error) {
	routes := []Route{}
	scanner := bufio.NewReader(input)
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			break
		}
		fields := strings.Fields(line)
		//关注以下字段：
		//0 - 目标地址
		//4 - 网关
		//9 - 接口名称
		dest, err := parseIP(fields[0], familyIPv6)
		if err != nil {
			return nil, err
		}
		gw, err := parseIP(fields[4], familyIPv6)
		if err != nil {
			return nil, err
		}
		if !dest.Equal(net.IPv6zero) {
			continue
		}
		if gw.Equal(net.IPv6zero) {
			continue //回环接口
		}
		routes = append(routes, Route{
			Interface:   fields[9],
			Destination: dest,
			Gateway:     gw,
			Family:      familyIPv6,
		})
	}
	return routes, nil
}

// parseIP 将路由文件中的十六进制 IP 字符串转换为 net.IP。
// IPv4 值必须转换为大端序。
//
// 中文说明：/proc 中的 IPv4 地址按主机字节序存储，需反转字节才能得到网络序；
// IPv6 已经是网络序，直接用。
func parseIP(str string, family AddressFamily) (net.IP, error) {
	if str == "" {
		return nil, fmt.Errorf("input is nil")
	}
	bytes, err := hex.DecodeString(str)
	if err != nil {
		return nil, err
	}
	if family == familyIPv4 {
		if len(bytes) != net.IPv4len {
			return nil, fmt.Errorf("invalid IPv4 address in route")
		}
		return net.IP([]byte{bytes[3], bytes[2], bytes[1], bytes[0]}), nil
	}
	//必须是 IPv6
	if len(bytes) != net.IPv6len {
		return nil, fmt.Errorf("invalid IPv6 address in route")
	}
	return net.IP(bytes), nil
}

// isInterfaceUp 判断接口是否处于 up 状态；nil 接口视为不可用。
func isInterfaceUp(logger klog.Logger, intf *net.Interface) bool {
	if intf == nil {
		return false
	}
	if intf.Flags&net.FlagUp != 0 {
		logger.V(4).Info("Interface is up", "interface", intf.Name)
		return true
	}
	return false
}

// isLoopbackOrPointToPoint 判断接口是否应被直接跳过：loopback 与点对点接口
// 的地址通常不能代表节点对外可达的 IP。
func isLoopbackOrPointToPoint(intf *net.Interface) bool {
	return intf.Flags&(net.FlagLoopback|net.FlagPointToPoint) != 0
}

// getMatchingGlobalIP 从 addrs 中返回指定地址族的第一有效全局单播地址。
func getMatchingGlobalIP(logger klog.Logger, addrs []net.Addr, family AddressFamily) (net.IP, error) {
	if len(addrs) > 0 {
		for i := range addrs {
			logger.V(4).Info("Checking for matching global IP", "address", addrs[i])
			ip, _, err := netutils.ParseCIDRSloppy(addrs[i].String())
			if err != nil {
				return nil, err
			}
			if memberOf(ip, family) {
				if ip.IsGlobalUnicast() {
					logger.V(4).Info("IP found", "IP", ip)
					return ip, nil
				} else {
					logger.V(4).Info("Non-global unicast address found", "IP", ip)
				}
			} else {
				logger.V(4).Info("IP address has wrong version", "IP", ip, "IPVersion", int(family))
			}

		}
	}
	// 没有匹配时返回 nil, nil 而不是错误：调用方会继续尝试其他接口。
	return nil, nil
}

// getIPFromInterface 获取接口上的 IP，并返回全局单播地址（如果有）。
// 接口必须启用，IP 必须属于请求的地址族且为全局单播地址。
func getIPFromInterface(logger klog.Logger, intfName string, forFamily AddressFamily, nw networkInterfacer) (net.IP, error) {
	intf, err := nw.InterfaceByName(intfName)
	if err != nil {
		return nil, err
	}
	if isInterfaceUp(logger, intf) {
		addrs, err := nw.Addrs(intf)
		if err != nil {
			return nil, err
		}
		logger.V(4).Info("Found addresses for interface", "interface", intfName, "numAddresses", len(addrs), "addresses", addrs)
		matchingIP, err := getMatchingGlobalIP(logger, addrs, forFamily)
		if err != nil {
			return nil, err
		}
		if matchingIP != nil {
			logger.V(4).Info("Found valid address", "IPVersion", int(forFamily), "IP", matchingIP, "interface", intfName)
			return matchingIP, nil
		}
	}
	return nil, nil
}

// getIPFromLoopbackInterface 获取回环接口上的 IP，并返回全局单播地址（如果有）。
// 回环接口必须启用，IP 必须属于请求的地址族且为全局单播地址。
//
// 中文说明：RFC5549 场景下默认路由的接口只有 link-local 地址，而真正的
// 全局地址挂在 loopback 上，因此在路由接口找不到时需回退到这里。
func getIPFromLoopbackInterface(logger klog.Logger, forFamily AddressFamily, nw networkInterfacer) (net.IP, error) {
	intfs, err := nw.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, intf := range intfs {
		if !isInterfaceUp(logger, &intf) {
			continue
		}
		if intf.Flags&(net.FlagLoopback) != 0 {
			addrs, err := nw.Addrs(&intf)
			if err != nil {
				return nil, err
			}
			logger.V(4).Info("Found addresses for interface", "interface", intf.Name, "numAddresses", len(addrs), "addresses", addrs)
			matchingIP, err := getMatchingGlobalIP(logger, addrs, forFamily)
			if err != nil {
				return nil, err
			}
			if matchingIP != nil {
				logger.V(4).Info("Found valid address", "IPVersion", int(forFamily), "IP", matchingIP, "interface", intf.Name)
				return matchingIP, nil
			}
		}
	}
	return nil, nil
}

// memberOf 判断 IP 是否属于目标地址族，用于检查接口地址。
func memberOf(ip net.IP, family AddressFamily) bool {
	if ip.To4() != nil {
		return family == familyIPv4
	} else {
		return family == familyIPv6
	}
}

// chooseIPFromHostInterfaces 遍历系统接口，寻找已启用且具有全局单播地址的接口，
// 该地址不能是回环、链路本地或点到点地址，并返回其 IP。
// addressFamilies 决定优先选择 IPv4 还是 IPv6。
func chooseIPFromHostInterfaces(logger klog.Logger, nw networkInterfacer, addressFamilies AddressFamilyPreference) (net.IP, error) {
	intfs, err := nw.Interfaces()
	if err != nil {
		return nil, err
	}
	if len(intfs) == 0 {
		return nil, fmt.Errorf("no interfaces found on host.")
	}
	for _, family := range addressFamilies {
		logger.V(4).Info("Looking for system interface with a global address", "IPVersion", uint(family))
		for _, intf := range intfs {
			if !isInterfaceUp(logger, &intf) {
				logger.V(4).Info("Skipping: interface is down", "interface", intf.Name)
				continue
			}
			if isLoopbackOrPointToPoint(&intf) {
				logger.V(4).Info("Skipping: is LB or P2P", "interface", intf.Name)
				continue
			}
			addrs, err := nw.Addrs(&intf)
			if err != nil {
				return nil, err
			}
			if len(addrs) == 0 {
				logger.V(4).Info("Skipping: no addresses", "interface", intf.Name)
				continue
			}
			for _, addr := range addrs {
				ip, _, err := netutils.ParseCIDRSloppy(addr.String())
				if err != nil {
					return nil, fmt.Errorf("unable to parse CIDR for interface %q: %s", intf.Name, err)
				}
				if !memberOf(ip, family) {
					logger.V(4).Info("Skipping: no address family match", "IP", ip, "interface", intf.Name)
					continue
				}
				//TODO: 决定未来是否允许 IPv6 链路本地地址。
				if !ip.IsGlobalUnicast() {
					logger.V(4).Info("Skipping: non-global address", "IP", ip, "interface", intf.Name)
					continue
				}
				logger.V(4).Info("Found global unicast address", "IP", ip, "interface", intf.Name)
				return ip, nil
			}
		}
	}
	return nil, fmt.Errorf("no acceptable interface with global unicast address found on host")
}

// ChooseHostInterface 为守护进程获取 IP。
// 没有路由信息文件时，从系统接口选择全局 IP；
// 否则使用 IPv4 和 IPv6 路由信息，返回具有网关的接口 IP（优先 IPv4）。
// 节点没有互联网连接时返回错误。
//
//logcheck:context // 支持上下文日志的代码应使用 [ChooseHostInterfaceWithLogger] 代替 ChooseHostInterface。
func ChooseHostInterface() (net.IP, error) {
	return ChooseHostInterfaceWithLogger(klog.Background())
}

// ChooseHostInterfaceWithLogger 为守护进程获取 IP，并支持上下文日志。
// 没有路由信息文件时，从系统接口选择全局 IP；
// 否则使用 IPv4 和 IPv6 路由信息，返回具有网关的接口 IP（优先 IPv4）。
// 节点没有互联网连接时返回错误。
//
// 中文说明：这是支持上下文日志的主入口；无 logger 的版本仅为兼容保留。
func ChooseHostInterfaceWithLogger(logger klog.Logger) (net.IP, error) {
	return chooseHostInterface(logger, preferIPv4)
}

// chooseHostInterface 决定探测策略：Linux 上若有 IPv4 路由文件则按默认路由
// 选地址，否则（或非 Linux）退化为遍历接口查找全局单播地址。
func chooseHostInterface(logger klog.Logger, addressFamilies AddressFamilyPreference) (net.IP, error) {
	var nw networkInterfacer = networkInterface{}
	if _, err := os.Stat(ipv4RouteFile); os.IsNotExist(err) {
		return chooseIPFromHostInterfaces(logger, nw, addressFamilies)
	}
	routes, err := getAllDefaultRoutes()
	if err != nil {
		return nil, err
	}
	return chooseHostInterfaceFromRoute(logger, routes, nw, addressFamilies)
}

// networkInterfacer 定义多个 net 库函数的接口。
// 生产代码调用 net 库函数，单元测试则覆盖这些方法。
type networkInterfacer interface {
	InterfaceByName(intfName string) (*net.Interface, error)
	Addrs(intf *net.Interface) ([]net.Addr, error)
	Interfaces() ([]net.Interface, error)
}

// networkInterface 为生产代码实现 networkInterfacer，仅包装底层 net 库调用。
type networkInterface struct{}

func (_ networkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return net.InterfaceByName(intfName)
}

func (_ networkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	return intf.Addrs()
}

func (_ networkInterface) Interfaces() ([]net.Interface, error) {
	return net.Interfaces()
}

// getAllDefaultRoutes 获取节点上的 IPv4 和 IPv6 默认路由。
// 无法读取 IPv4 路由信息文件时返回错误；IPv6 路由信息文件是可选的，
// 无法读取时仅使用 IPv4 路由信息。
// 如果所有路由信息中都没有默认路由，则返回错误。
func getAllDefaultRoutes() ([]Route, error) {
	routes, err := v4File.extract()
	if err != nil {
		return nil, err
	}
	// IPv6 路由文件是可选的：缺失或不可读时忽略，不阻塞 IPv4 结果。
	v6Routes, _ := v6File.extract()
	routes = append(routes, v6Routes...)
	if len(routes) == 0 {
		return nil, noRoutesError{
			message: fmt.Sprintf("no default routes found in %q or %q", v4File.name, v6File.name),
		}
	}
	return routes, nil
}

// chooseHostInterfaceFromRoute 遍历默认路由，从对应接口寻找全局 IP。
// 如果有路由但接口没有全局地址，
// 则检查回环接口是否有全局地址。
// addressFamilies 决定优先选择 IPv4 还是 IPv6。
func chooseHostInterfaceFromRoute(logger klog.Logger, routes []Route, nw networkInterfacer, addressFamilies AddressFamilyPreference) (net.IP, error) {
	for _, family := range addressFamilies {
		logger.V(4).Info("Looking for default routes with IP addresses", "IPVersion", uint(family))
		for _, route := range routes {
			if route.Family != family {
				continue
			}
			logger.V(4).Info("Default route transits interface", "interface", route.Interface)
			finalIP, err := getIPFromInterface(logger, route.Interface, family, nw)
			if err != nil {
				return nil, err
			}
			if finalIP != nil {
				logger.V(4).Info("Found active IP", "IP", finalIP)
				return finalIP, nil
			}
			//在存在默认路由但接口仅使用链路本地地址的部署中（如 RFC 5549 所述），
			//需要此函数。
			//全局 IP 被分配给回环接口，因此应使用该地址。
			loopbackIP, err := getIPFromLoopbackInterface(logger, family, nw)
			if err != nil {
				return nil, err
			}
			if loopbackIP != nil {
				logger.V(4).Info("Found active IP on Loopback interface", "IP", loopbackIP)
				return loopbackIP, nil
			}
		}
	}
	logger.V(4).Info("No active IP found by looking at default routes")
	return nil, fmt.Errorf("unable to select an IP from default routes.")
}

// ResolveBindAddress 根据给定 bindAddress 返回守护进程 IP：
// bindAddress 未设置时，与 ChooseHostInterface() 一样返回主机默认 IP。
// bindAddress 为未指定地址或回环地址时，返回相同地址族的默认 IP。
//
// 否则直接返回 bindAddress。
//
//logcheck:context // 支持上下文日志的代码应使用 [ResolveBindAddressWithLogger] 代替 ResolveBindAddress。
func ResolveBindAddress(bindAddress net.IP) (net.IP, error) {
	return ResolveBindAddressWithLogger(klog.Background(), bindAddress)
}

// ResolveBindAddressWithLogger 根据给定 bindAddress 返回守护进程 IP，并支持上下文日志：
// bindAddress 未设置时，与 ChooseHostInterface() 一样返回主机默认 IP。
// bindAddress 为未指定地址或回环地址时，返回相同地址族的默认 IP。
// 否则直接返回 bindAddress。
//
// 中文说明：绑定到 unspecified（0.0.0.0/::）或 loopback 时，实际对外地址仍需
// 从主机接口选择，且保持与用户指定地址相同的族（IPv4/IPv6）。
func ResolveBindAddressWithLogger(logger klog.Logger, bindAddress net.IP) (net.IP, error) {
	addressFamilies := preferIPv4
	if bindAddress != nil && memberOf(bindAddress, familyIPv6) {
		addressFamilies = preferIPv6
	}

	if bindAddress == nil || bindAddress.IsUnspecified() || bindAddress.IsLoopback() {
		hostIP, err := chooseHostInterface(logger, addressFamilies)
		if err != nil {
			return nil, err
		}
		bindAddress = hostIP
	}
	return bindAddress, nil
}

// ChooseBindAddressForInterface 为指定接口选择全局 IP，优先 IPv4。
// 在存在默认路由但网络接口仅使用链路本地地址（如 RFC 5549 所述）的部署中，
// 需要此函数。
// 例如通过 BGP 在链路本地 IP 上宣告主机 IP，而该 IP 附加在 lo 接口上。
//
//logcheck:context // 支持上下文日志的代码应使用 [ChooseBindAddressForInterfaceWithLogger] 代替 ChooseBindAddressForInterface。
func ChooseBindAddressForInterface(intfName string) (net.IP, error) {
	return ChooseBindAddressForInterfaceWithLogger(klog.Background(), intfName)
}

// ChooseBindAddressForInterfaceWithLogger 为指定接口选择全局 IP，优先 IPv4，并支持上下文日志。
// 在存在默认路由但网络接口仅使用链路本地地址（如 RFC 5549 所述）的部署中，
// 需要此函数。
// 例如通过 BGP 在链路本地 IP 上宣告主机 IP，而该 IP 附加在 lo 接口上。
func ChooseBindAddressForInterfaceWithLogger(logger klog.Logger, intfName string) (net.IP, error) {
	var nw networkInterfacer = networkInterface{}
	for _, family := range preferIPv4 {
		ip, err := getIPFromInterface(logger, intfName, family, nw)
		if err != nil {
			return nil, err
		}
		if ip != nil {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("unable to select an IP from %s network interface", intfName)
}
