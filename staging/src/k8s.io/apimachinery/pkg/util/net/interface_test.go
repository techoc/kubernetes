/*
Copyright 2014 The Kubernetes Authors.

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

package net

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"k8s.io/klog/v2/ktesting"
	netutils "k8s.io/utils/net"
)

// 以下常量为 /proc/net/route（IPv4）与 /proc/net/ipv6_route（IPv6）的样例内容，
// 覆盖默认路由位于首行、中间、末行，以及无默认路由、地址非法、十六进制长度非法等情形。
const gatewayfirst = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT                                                       
eth3	00000000	0100FE0A	0003	0	0	1024	00000000	0	0	0                                                                   
eth3	0000FE0A	00000000	0001	0	0	0	0080FFFF	0	0	0                                                                      
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0                                                                            
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0
`
const gatewaylast = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT  
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0                                                                            
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0                                                                                                                     
eth3	0000FE0A	00000000	0001	0	0	0	0080FFFF	0	0	0       
eth3	00000000	0100FE0A	0003	0	0	1024	00000000	0	0	0                                                                 
`
const gatewaymiddle = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT                                                                                                                     
eth3	0000FE0A	00000000	0001	0	0	0	0080FFFF	0	0	0                                                                      
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0       
eth3	00000000	0100FE0A	0003	0	0	1024	00000000	0	0	0                                                                         
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0
`
const noInternetConnection = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT                                                       
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0                                                                            
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0            
`
const nothing = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT                                                            
`
const badDestination = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT                                                       
eth3	00000000	0100FE0A	0003	0	0	1024	00000000	0	0	0                                                                   
eth3	0000FE0AA1	00000000	0001	0	0	0	0080FFFF	0	0	0                                                                      
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0                                                                            
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0
`
const badGateway = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth3	00000000	0100FE0AA1	0003	0	0	1024	00000000	0	0	0                                                                   
eth3	0000FE0A	00000000	0001	0	0	0	0080FFFF	0	0	0                                                                      
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0                                                                            
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0
`
const route_Invalidhex = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth3	00000000	0100FE0AA	0003	0	0	1024	00000000	0	0	0                                                                   
eth3	0000FE0A	00000000	0001	0	0	0	0080FFFF	0	0	0                                                                      
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0                                                                            
virbr0	007AA8C0	00000000	0001	0	0	0	00FFFFFF	0	0	0
`

const v6gatewayfirst = `00000000000000000000000000000000 00 00000000000000000000000000000000 00 20010001000000000000000000000001 00000064 00000000 00000000 00000003 eth3
20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001 eth3
00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo
`
const v6gatewaylast = `20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001 eth3
00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo
00000000000000000000000000000000 00 00000000000000000000000000000000 00 20010001000000000000000000000001 00000064 00000000 00000000 00000003 eth3
`
const v6gatewaymiddle = `20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001 eth3
00000000000000000000000000000000 00 00000000000000000000000000000000 00 20010001000000000000000000000001 00000064 00000000 00000000 00000003 eth3
00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo
`
const v6noDefaultRoutes = `00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo
20010001000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00000001  docker0
20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001   eth3
fe800000000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001   eth3
`
const v6nothing = ``
const v6badDestination = `2001000200000000 7a 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo
`
const v6badGateway = `00000000000000000000000000000000 00 00000000000000000000000000000000 00 200100010000000000000000000000000012 00000064 00000000 00000000 00000003 eth3
`
const v6route_Invalidhex = `000000000000000000000000000000000 00 00000000000000000000000000000000 00 fe80000000000000021fcafffea0ec00 00000064 00000000 00000000 00000003 enp1s0f0

`

// 网卡标志位组合，模拟 up/down/loopback/point-to-point 四类接口。
const (
	flagUp       = net.FlagUp | net.FlagBroadcast | net.FlagMulticast
	flagDown     = net.FlagBroadcast | net.FlagMulticast
	flagLoopback = net.FlagUp | net.FlagLoopback
	flagP2P      = net.FlagUp | net.FlagPointToPoint
)

// makeIntf 构造测试用网卡，MAC 地址随 index 变化以便区分。
func makeIntf(index int, name string, flags net.Flags) net.Interface {
	mac := net.HardwareAddr{0, 0x32, 0x7d, 0x69, 0xf7, byte(0x30 + index)}
	return net.Interface{
		Index:        index,
		MTU:          1500,
		Name:         name,
		HardwareAddr: mac,
		Flags:        flags}
}

// 预置的网卡与默认路由样例，供各测试用例复用。
var (
	downIntf     = makeIntf(1, "eth3", flagDown)
	loopbackIntf = makeIntf(1, "lo", flagLoopback)
	p2pIntf      = makeIntf(1, "lo", flagP2P)
	upIntf       = makeIntf(1, "eth3", flagUp)
)

var (
	ipv4Route = Route{Interface: "eth3", Destination: netutils.ParseIPSloppy("0.0.0.0"), Gateway: netutils.ParseIPSloppy("10.254.0.1"), Family: familyIPv4}
	ipv6Route = Route{Interface: "eth3", Destination: netutils.ParseIPSloppy("::"), Gateway: netutils.ParseIPSloppy("2001:1::1"), Family: familyIPv6}
)

var (
	noRoutes   = []Route{}
	routeV4    = []Route{ipv4Route}
	routeV6    = []Route{ipv6Route}
	bothRoutes = []Route{ipv4Route, ipv6Route}
)

// TestGetIPv4Routes 验证从 /proc/net/route 解析默认路由：
// 无论默认路由在首/中/末行都能识别；非法地址或非法十六进制返回错误；无默认路由时返回空。
func TestGetIPv4Routes(t *testing.T) {
	testCases := []struct {
		tcase      string
		route      string
		count      int
		expected   *Route
		errStrFrag string
	}{
		{"gatewayfirst", gatewayfirst, 1, &ipv4Route, ""},
		{"gatewaymiddle", gatewaymiddle, 1, &ipv4Route, ""},
		{"gatewaylast", gatewaylast, 1, &ipv4Route, ""},
		{"no routes", nothing, 0, nil, ""},
		{"badDestination", badDestination, 0, nil, "invalid IPv4"},
		{"badGateway", badGateway, 0, nil, "invalid IPv4"},
		{"route_Invalidhex", route_Invalidhex, 0, nil, "odd length hex string"},
		{"no default routes", noInternetConnection, 0, nil, ""},
	}
	for _, tc := range testCases {
		r := strings.NewReader(tc.route)
		routes, err := getIPv4DefaultRoutes(r)
		if err != nil {
			if !strings.Contains(err.Error(), tc.errStrFrag) {
				t.Errorf("case[%s]: Error string %q does not contain %q", tc.tcase, err, tc.errStrFrag)
			}
		} else if tc.errStrFrag != "" {
			t.Errorf("case[%s]: Error %q expected, but not seen", tc.tcase, tc.errStrFrag)
		} else {
			if tc.count != len(routes) {
				t.Errorf("case[%s]: expected %d routes, have %v", tc.tcase, tc.count, routes)
			} else if tc.count == 1 {
				if !tc.expected.Gateway.Equal(routes[0].Gateway) {
					t.Errorf("case[%s]: expected %v, got %v .err : %v", tc.tcase, tc.expected, routes, err)
				}
				if !routes[0].Destination.Equal(net.IPv4zero) {
					t.Errorf("case[%s}: destination is not for default route (not zero)", tc.tcase)
				}

			}
		}
	}
}

// TestGetIPv6Routes 验证从 /proc/net/ipv6_route 解析默认路由，覆盖与 IPv4 相同的行位置与非法输入场景。
func TestGetIPv6Routes(t *testing.T) {
	testCases := []struct {
		tcase      string
		route      string
		count      int
		expected   *Route
		errStrFrag string
	}{
		{"v6 gatewayfirst", v6gatewayfirst, 1, &ipv6Route, ""},
		{"v6 gatewaymiddle", v6gatewaymiddle, 1, &ipv6Route, ""},
		{"v6 gatewaylast", v6gatewaylast, 1, &ipv6Route, ""},
		{"v6 no routes", v6nothing, 0, nil, ""},
		{"v6 badDestination", v6badDestination, 0, nil, "invalid IPv6"},
		{"v6 badGateway", v6badGateway, 0, nil, "invalid IPv6"},
		{"v6 route_Invalidhex", v6route_Invalidhex, 0, nil, "odd length hex string"},
		{"v6 no default routes", v6noDefaultRoutes, 0, nil, ""},
	}
	for _, tc := range testCases {
		r := strings.NewReader(tc.route)
		routes, err := getIPv6DefaultRoutes(r)
		if err != nil {
			if !strings.Contains(err.Error(), tc.errStrFrag) {
				t.Errorf("case[%s]: Error string %q does not contain %q", tc.tcase, err, tc.errStrFrag)
			}
		} else if tc.errStrFrag != "" {
			t.Errorf("case[%s]: Error %q expected, but not seen", tc.tcase, tc.errStrFrag)
		} else {
			if tc.count != len(routes) {
				t.Errorf("case[%s]: expected %d routes, have %v", tc.tcase, tc.count, routes)
			} else if tc.count == 1 {
				if !tc.expected.Gateway.Equal(routes[0].Gateway) {
					t.Errorf("case[%s]: expected %v, got %v .err : %v", tc.tcase, tc.expected, routes, err)
				}
				if !routes[0].Destination.Equal(net.IPv6zero) {
					t.Errorf("case[%s}: destination is not for default route (not zero)", tc.tcase)
				}
			}
		}
	}
}

// TestParseIP 验证把十六进制地址字符串解析为 net.IP：长度必须与地址族匹配，内容非法时返回 nil。
func TestParseIP(t *testing.T) {
	testCases := []struct {
		tcase    string
		ip       string
		family   AddressFamily
		success  bool
		expected net.IP
	}{
		{"empty", "", familyIPv4, false, nil},
		{"too short", "AA", familyIPv4, false, nil},
		{"too long", "0011223344", familyIPv4, false, nil},
		{"invalid", "invalid!", familyIPv4, false, nil},
		{"zero", "00000000", familyIPv4, true, net.IP{0, 0, 0, 0}},
		{"ffff", "FFFFFFFF", familyIPv4, true, net.IP{0xff, 0xff, 0xff, 0xff}},
		{"valid v4", "12345678", familyIPv4, true, net.IP{120, 86, 52, 18}},
		{"valid v6", "fe800000000000000000000000000000", familyIPv6, true, net.IP{0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"v6 too short", "fe80000000000000021fcafffea0ec0", familyIPv6, false, nil},
		{"v6 too long", "fe80000000000000021fcafffea0ec002", familyIPv6, false, nil},
	}
	for _, tc := range testCases {
		ip, err := parseIP(tc.ip, tc.family)
		if !ip.Equal(tc.expected) {
			t.Errorf("case[%v]: expected %q, got %q . err : %v", tc.tcase, tc.expected, ip, err)
		}
	}
}

// TestIsInterfaceUp 验证只有带 FlagUp 的网卡被视为 up，nil 网卡返回 false。
func TestIsInterfaceUp(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		tcase    string
		intf     *net.Interface
		expected bool
	}{
		{"up", &net.Interface{Index: 0, MTU: 0, Name: "eth3", HardwareAddr: nil, Flags: net.FlagUp}, true},
		{"down", &net.Interface{Index: 0, MTU: 0, Name: "eth3", HardwareAddr: nil, Flags: 0}, false},
		{"no interface", nil, false},
	}
	for _, tc := range testCases {
		it := isInterfaceUp(logger, tc.intf)
		if it != tc.expected {
			t.Errorf("case[%v]: expected %v, got %v .", tc.tcase, tc.expected, it)
		}
	}
}

// addrStruct 是实现了 net.Addr 的测试地址，仅携带字符串形式的值。
type addrStruct struct{ val string }

func (a addrStruct) Network() string {
	return a.val
}
func (a addrStruct) String() string {
	return a.val
}

// TestFinalIP 验证从网卡地址中挑选可用全局 IP 时跳过 loopback、link-local 与非法 CIDR，
// 且只返回指定地址族的地址。
func TestFinalIP(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		tcase    string
		addr     []net.Addr
		family   AddressFamily
		expected net.IP
	}{
		{"no ipv4", []net.Addr{addrStruct{val: "2001::5/64"}}, familyIPv4, nil},
		{"no ipv6", []net.Addr{addrStruct{val: "10.128.0.4/32"}}, familyIPv6, nil},
		{"invalidV4CIDR", []net.Addr{addrStruct{val: "10.20.30.40.50/24"}}, familyIPv4, nil},
		{"invalidV6CIDR", []net.Addr{addrStruct{val: "fe80::2f7:67fff:fe6e:2956/64"}}, familyIPv6, nil},
		{"loopback", []net.Addr{addrStruct{val: "127.0.0.1/24"}}, familyIPv4, nil},
		{"loopbackv6", []net.Addr{addrStruct{val: "::1/128"}}, familyIPv6, nil},
		{"link local v4", []net.Addr{addrStruct{val: "169.254.1.10/16"}}, familyIPv4, nil},
		{"link local v6", []net.Addr{addrStruct{val: "fe80::2f7:6fff:fe6e:2956/64"}}, familyIPv6, nil},
		{"ip4", []net.Addr{addrStruct{val: "10.254.12.132/17"}}, familyIPv4, netutils.ParseIPSloppy("10.254.12.132")},
		{"ip6", []net.Addr{addrStruct{val: "2001::5/64"}}, familyIPv6, netutils.ParseIPSloppy("2001::5")},

		{"no addresses", []net.Addr{}, familyIPv4, nil},
	}
	for _, tc := range testCases {
		ip, err := getMatchingGlobalIP(logger, tc.addr, tc.family)
		if !ip.Equal(tc.expected) {
			t.Errorf("case[%v]: expected %v, got %v .err : %v", tc.tcase, tc.expected, ip, err)
		}
	}
}

// TestAddrs 验证 networkInterfacer 的 Addrs 包装返回底层地址列表。
func TestAddrs(t *testing.T) {
	var nw networkInterfacer = validNetworkInterface{}
	intf := net.Interface{Index: 0, MTU: 0, Name: "eth3", HardwareAddr: nil, Flags: 0}
	addrs, err := nw.Addrs(&intf)
	if err != nil {
		t.Errorf("expected no error got : %v", err)
	}
	if len(addrs) != 2 {
		t.Errorf("expected addrs: 2 got null")
	}
}

// validNetworkInterface 有一个全局 IPv4 地址（IPv6 仅 link-local）。
type validNetworkInterface struct {
}

func (_ validNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ validNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{
		addrStruct{val: "fe80::2f7:6fff:fe6e:2956/64"}, addrStruct{val: "10.254.71.145/17"}}
	return ifat, nil
}
func (_ validNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// v4v6NetworkInterface 同时有 IPv4 与 IPv6 全局地址（用于验证地址族偏好）。
type v4v6NetworkInterface struct {
}

func (_ v4v6NetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ v4v6NetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{
		addrStruct{val: "2001::10/64"}, addrStruct{val: "10.254.71.145/17"}}
	return ifat, nil
}
func (_ v4v6NetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// ipv6NetworkInterface 只有 IPv6 全局地址。
type ipv6NetworkInterface struct {
}

func (_ ipv6NetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ ipv6NetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{addrStruct{val: "2001::200/64"}}
	return ifat, nil
}

func (_ ipv6NetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// networkInterfaceWithOnlyLinkLocals 只有 link-local 地址。
type networkInterfaceWithOnlyLinkLocals struct {
}

func (_ networkInterfaceWithOnlyLinkLocals) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ networkInterfaceWithOnlyLinkLocals) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{addrStruct{val: "169.254.162.166/16"}, addrStruct{val: "fe80::200/10"}}
	return ifat, nil
}
func (_ networkInterfaceWithOnlyLinkLocals) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// failGettingNetworkInterface 模拟获取网卡失败。
type failGettingNetworkInterface struct {
}

func (_ failGettingNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return nil, fmt.Errorf("unable get Interface")
}
func (_ failGettingNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	return nil, nil
}
func (_ failGettingNetworkInterface) Interfaces() ([]net.Interface, error) {
	return nil, fmt.Errorf("mock failed getting all interfaces")
}

// noNetworkInterface 模拟系统中没有任何网卡。
type noNetworkInterface struct {
}

func (_ noNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return nil, fmt.Errorf("no such network interface")
}
func (_ noNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	return nil, nil
}
func (_ noNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{}, nil
}

// downNetworkInterface 模拟处于 down 状态的网卡。
type downNetworkInterface struct {
}

func (_ downNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return &downIntf, nil
}
func (_ downNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{
		addrStruct{val: "fe80::2f7:6fff:fe6e:2956/64"}, addrStruct{val: "10.254.71.145/17"}}
	return ifat, nil
}
func (_ downNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{downIntf}, nil
}

// loopbackNetworkInterface 模拟只有 loopback 网卡。
type loopbackNetworkInterface struct {
}

func (_ loopbackNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return &loopbackIntf, nil
}
func (_ loopbackNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{
		addrStruct{val: "::1/128"}, addrStruct{val: "127.0.0.1/8"}}
	return ifat, nil
}
func (_ loopbackNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{loopbackIntf}, nil
}

// p2pNetworkInterface 模拟 point-to-point 网卡。
type p2pNetworkInterface struct {
}

func (_ p2pNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	return &p2pIntf, nil
}
func (_ p2pNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{
		addrStruct{val: "::1/128"}, addrStruct{val: "127.0.0.1/8"}}
	return ifat, nil
}
func (_ p2pNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{p2pIntf}, nil
}

// linkLocalLoopbackNetworkInterface 的普通网卡只有 link-local 地址，
// loopback 网卡上配置了全局地址（用于验证回退到 loopback 全局地址）。
type linkLocalLoopbackNetworkInterface struct {
}

func (_ linkLocalLoopbackNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	if intfName == LoopbackInterfaceName {
		return &loopbackIntf, nil
	}
	return &upIntf, nil
}
func (_ linkLocalLoopbackNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{addrStruct{val: "169.254.162.166/16"}, addrStruct{val: "fe80::200/10"}}
	if intf.Name == LoopbackInterfaceName {
		ifat = []net.Addr{addrStruct{val: "::1/128"}, addrStruct{val: "127.0.0.1/8"},
			// global addresses on loopback interface
			addrStruct{val: "10.1.1.1/32"}, addrStruct{val: "fd00:1:1::1/128"}}
	}
	return ifat, nil
}
func (_ linkLocalLoopbackNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf, loopbackIntf}, nil
}

// globalsNetworkInterface 的普通网卡与 loopback 网卡都配置了全局地址。
type globalsNetworkInterface struct {
}

func (_ globalsNetworkInterface) InterfaceByName(intfName string) (*net.Interface, error) {
	if intfName == LoopbackInterfaceName {
		return &loopbackIntf, nil
	}
	return &upIntf, nil
}
func (_ globalsNetworkInterface) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{addrStruct{val: "169.254.162.166/16"}, addrStruct{val: "fe80::200/10"},
		addrStruct{val: "192.168.1.1/31"}, addrStruct{val: "fd00::200/127"}}
	if intf.Name == LoopbackInterfaceName {
		ifat = []net.Addr{addrStruct{val: "::1/128"}, addrStruct{val: "127.0.0.1/8"},
			// global addresses on loopback interface
			addrStruct{val: "10.1.1.1/32"}, addrStruct{val: "fd00:1:1::1/128"}}
	}
	return ifat, nil
}
func (_ globalsNetworkInterface) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf, loopbackIntf}, nil
}

// networkInterfaceFailGetAddrs 模拟获取网卡地址失败。
type networkInterfaceFailGetAddrs struct {
}

func (_ networkInterfaceFailGetAddrs) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ networkInterfaceFailGetAddrs) Addrs(intf *net.Interface) ([]net.Addr, error) {
	return nil, fmt.Errorf("unable to get Addrs")
}
func (_ networkInterfaceFailGetAddrs) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// networkInterfaceWithNoAddrs 模拟网卡没有任何地址。
type networkInterfaceWithNoAddrs struct {
}

func (_ networkInterfaceWithNoAddrs) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ networkInterfaceWithNoAddrs) Addrs(intf *net.Interface) ([]net.Addr, error) {
	ifat := []net.Addr{}
	return ifat, nil
}
func (_ networkInterfaceWithNoAddrs) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// networkInterfaceWithInvalidAddr 模拟网卡地址无法解析为 CIDR。
type networkInterfaceWithInvalidAddr struct {
}

func (_ networkInterfaceWithInvalidAddr) InterfaceByName(intfName string) (*net.Interface, error) {
	return &upIntf, nil
}
func (_ networkInterfaceWithInvalidAddr) Addrs(intf *net.Interface) ([]net.Addr, error) {
	var ifat []net.Addr
	ifat = []net.Addr{addrStruct{val: "10.20.30.40.50/24"}}
	return ifat, nil
}
func (_ networkInterfaceWithInvalidAddr) Interfaces() ([]net.Interface, error) {
	return []net.Interface{upIntf}, nil
}

// TestGetIPFromInterface 验证从指定网卡获取全局 IP：
// 跳过 down 网卡、无匹配地址族、非法 CIDR，并在获取失败时返回对应错误。
func TestGetIPFromInterface(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		tcase      string
		nwname     string
		family     AddressFamily
		nw         networkInterfacer
		expected   net.IP
		errStrFrag string
	}{
		{"ipv4", "eth3", familyIPv4, validNetworkInterface{}, netutils.ParseIPSloppy("10.254.71.145"), ""},
		{"ipv6", "eth3", familyIPv6, ipv6NetworkInterface{}, netutils.ParseIPSloppy("2001::200"), ""},
		{"no ipv4", "eth3", familyIPv4, ipv6NetworkInterface{}, nil, ""},
		{"no ipv6", "eth3", familyIPv6, validNetworkInterface{}, nil, ""},
		{"I/F down", "eth3", familyIPv4, downNetworkInterface{}, nil, ""},
		{"I/F get fail", "eth3", familyIPv4, noNetworkInterface{}, nil, "no such network interface"},
		{"fail get addr", "eth3", familyIPv4, networkInterfaceFailGetAddrs{}, nil, "unable to get Addrs"},
		{"bad addr", "eth3", familyIPv4, networkInterfaceWithInvalidAddr{}, nil, "invalid CIDR"},
	}
	for _, tc := range testCases {
		ip, err := getIPFromInterface(logger, tc.nwname, tc.family, tc.nw)
		if err != nil {
			if !strings.Contains(err.Error(), tc.errStrFrag) {
				t.Errorf("case[%s]: Error string %q does not contain %q", tc.tcase, err, tc.errStrFrag)
			}
		} else if tc.errStrFrag != "" {
			t.Errorf("case[%s]: Error %q expected, but not seen", tc.tcase, tc.errStrFrag)
		} else if !ip.Equal(tc.expected) {
			t.Errorf("case[%v]: expected %v, got %+v .err : %v", tc.tcase, tc.expected, ip, err)
		}
	}
}

// TestGetIPFromLoopbackInterface 验证 loopback 网卡上的全局地址可作为回退选择，
// 仅有 loopback 地址时返回 nil。
func TestGetIPFromLoopbackInterface(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		tcase      string
		family     AddressFamily
		nw         networkInterfacer
		expected   net.IP
		errStrFrag string
	}{
		{"ipv4", familyIPv4, linkLocalLoopbackNetworkInterface{}, netutils.ParseIPSloppy("10.1.1.1"), ""},
		{"ipv6", familyIPv6, linkLocalLoopbackNetworkInterface{}, netutils.ParseIPSloppy("fd00:1:1::1"), ""},
		{"no global ipv4", familyIPv4, loopbackNetworkInterface{}, nil, ""},
		{"no global ipv6", familyIPv6, loopbackNetworkInterface{}, nil, ""},
	}
	for _, tc := range testCases {
		ip, err := getIPFromLoopbackInterface(logger, tc.family, tc.nw)
		if err != nil {
			if !strings.Contains(err.Error(), tc.errStrFrag) {
				t.Errorf("case[%s]: Error string %q does not contain %q", tc.tcase, err, tc.errStrFrag)
			}
		} else if tc.errStrFrag != "" {
			t.Errorf("case[%s]: Error %q expected, but seen %v", tc.tcase, tc.errStrFrag, err)
		} else if !ip.Equal(tc.expected) {
			t.Errorf("case[%v]: expected %v, got %+v .err : %v", tc.tcase, tc.expected, ip, err)
		}
	}
}

// TestChooseHostInterfaceFromRoute 验证依据默认路由选择主机 IP：
// 覆盖单栈/双栈、地址族偏好、link-local 回退到 loopback、无路由与获取失败等情形。
func TestChooseHostInterfaceFromRoute(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		tcase    string
		routes   []Route
		nw       networkInterfacer
		order    AddressFamilyPreference
		expected net.IP
	}{
		{"single-stack ipv4", routeV4, validNetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("10.254.71.145")},
		{"single-stack ipv4, prefer v6", routeV4, validNetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("10.254.71.145")},
		{"single-stack ipv6", routeV6, ipv6NetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("2001::200")},
		{"single-stack ipv6, prefer v6", routeV6, ipv6NetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("2001::200")},
		{"dual stack", bothRoutes, v4v6NetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("10.254.71.145")},
		{"dual stack, prefer v6", bothRoutes, v4v6NetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("2001::10")},
		{"LLA and loopback with global, IPv4", routeV4, linkLocalLoopbackNetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("10.1.1.1")},
		{"LLA and loopback with global, IPv6", routeV6, linkLocalLoopbackNetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("fd00:1:1::1")},
		{"LLA and loopback with global, dual stack prefer IPv4", bothRoutes, linkLocalLoopbackNetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("10.1.1.1")},
		{"LLA and loopback with global, dual stack prefer IPv6", bothRoutes, linkLocalLoopbackNetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("fd00:1:1::1")},
		{"LLA and loopback with global, no routes", noRoutes, linkLocalLoopbackNetworkInterface{}, preferIPv6, nil},
		{"interface and loopback with global, IPv4", routeV4, globalsNetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("192.168.1.1")},
		{"interface and loopback with global, IPv6", routeV6, globalsNetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("fd00::200")},
		{"interface and loopback with global, dual stack prefer IPv4", bothRoutes, globalsNetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("192.168.1.1")},
		{"interface and loopback with global, dual stack prefer IPv6", bothRoutes, globalsNetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("fd00::200")},
		{"interface and loopback with global, no routes", noRoutes, globalsNetworkInterface{}, preferIPv6, nil},
		{"all LLA", routeV4, networkInterfaceWithOnlyLinkLocals{}, preferIPv4, nil},
		{"no routes", noRoutes, validNetworkInterface{}, preferIPv4, nil},
		{"fail get IP", routeV4, networkInterfaceFailGetAddrs{}, preferIPv4, nil},
	}
	for _, tc := range testCases {
		ip, err := chooseHostInterfaceFromRoute(logger, tc.routes, tc.nw, tc.order)
		if !ip.Equal(tc.expected) {
			t.Errorf("case[%v]: expected %v, got %+v .err : %v", tc.tcase, tc.expected, ip, err)
		}
	}
}

// TestMemberOf 验证地址族归属判断：IPv4 地址只属于 familyIPv4，IPv6 地址只属于 familyIPv6。
func TestMemberOf(t *testing.T) {
	testCases := []struct {
		tcase    string
		ip       net.IP
		family   AddressFamily
		expected bool
	}{
		{"ipv4 is 4", netutils.ParseIPSloppy("10.20.30.40"), familyIPv4, true},
		{"ipv4 is 6", netutils.ParseIPSloppy("10.10.10.10"), familyIPv6, false},
		{"ipv6 is 4", netutils.ParseIPSloppy("2001::100"), familyIPv4, false},
		{"ipv6 is 6", netutils.ParseIPSloppy("2001::100"), familyIPv6, true},
	}
	for _, tc := range testCases {
		if memberOf(tc.ip, tc.family) != tc.expected {
			t.Errorf("case[%s]: expected %+v", tc.tcase, tc.expected)
		}
	}
}

// TestGetIPFromHostInterfaces 验证遍历所有网卡选择 IP 的行为：
// 跳过 down、loopback-only、p2p、无地址或非法地址的网卡，并按地址族偏好返回结果。
func TestGetIPFromHostInterfaces(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		tcase      string
		nw         networkInterfacer
		order      AddressFamilyPreference
		expected   net.IP
		errStrFrag string
	}{
		{"fail get I/Fs", failGettingNetworkInterface{}, preferIPv4, nil, "failed getting all interfaces"},
		{"no interfaces", noNetworkInterface{}, preferIPv4, nil, "no interfaces"},
		{"I/F not up", downNetworkInterface{}, preferIPv4, nil, "no acceptable"},
		{"loopback only", loopbackNetworkInterface{}, preferIPv4, nil, "no acceptable"},
		{"P2P I/F only", p2pNetworkInterface{}, preferIPv4, nil, "no acceptable"},
		{"fail get addrs", networkInterfaceFailGetAddrs{}, preferIPv4, nil, "unable to get Addrs"},
		{"no addresses", networkInterfaceWithNoAddrs{}, preferIPv4, nil, "no acceptable"},
		{"invalid addr", networkInterfaceWithInvalidAddr{}, preferIPv4, nil, "invalid CIDR"},
		{"no matches", networkInterfaceWithOnlyLinkLocals{}, preferIPv4, nil, "no acceptable"},
		{"single-stack ipv4", validNetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("10.254.71.145"), ""},
		{"single-stack ipv4, prefer ipv6", validNetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("10.254.71.145"), ""},
		{"single-stack ipv6", ipv6NetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("2001::200"), ""},
		{"single-stack ipv6, prefer ipv6", ipv6NetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("2001::200"), ""},
		{"dual stack", v4v6NetworkInterface{}, preferIPv4, netutils.ParseIPSloppy("10.254.71.145"), ""},
		{"dual stack, prefer ipv6", v4v6NetworkInterface{}, preferIPv6, netutils.ParseIPSloppy("2001::10"), ""},
	}

	for _, tc := range testCases {
		ip, err := chooseIPFromHostInterfaces(logger, tc.nw, tc.order)
		if !ip.Equal(tc.expected) {
			t.Errorf("case[%s]: expected %+v, got %+v with err : %v", tc.tcase, tc.expected, ip, err)
		}
		if err != nil && !strings.Contains(err.Error(), tc.errStrFrag) {
			t.Errorf("case[%s]: unable to find %q in error string %q", tc.tcase, tc.errStrFrag, err.Error())
		}
	}
}

// makeRouteFile 把路由表内容写入临时文件，供读取路由文件的测试使用。
func makeRouteFile(content string, t *testing.T) (*os.File, error) {
	routeFile, err := os.CreateTemp("", "route")
	if err != nil {
		return nil, err
	}

	if _, err := routeFile.Write([]byte(content)); err != nil {
		return routeFile, err
	}
	err = routeFile.Close()
	return routeFile, err
}

// TestFailGettingIPv4Routes 验证读取不存在的 IPv4 路由文件时返回错误。
func TestFailGettingIPv4Routes(t *testing.T) {
	defer func() { v4File.name = ipv4RouteFile }()

	// Try failure to open file (should not occur, as caller ensures we have IPv4 route file, but being thorough)
	v4File.name = "no-such-file"
	errStrFrag := "no such file"
	_, err := v4File.extract()
	if err == nil {
		t.Errorf("Expected error trying to read non-existent v4 route file")
	}
	if !strings.Contains(err.Error(), errStrFrag) {
		t.Errorf("Unable to find %q in error string %q", errStrFrag, err.Error())
	}
}

// TestFailGettingIPv6Routes 验证读取不存在的 IPv6 路由文件时返回错误。
func TestFailGettingIPv6Routes(t *testing.T) {
	defer func() { v6File.name = ipv6RouteFile }()

	// Try failure to open file (this would be ignored by caller)
	v6File.name = "no-such-file"
	errStrFrag := "no such file"
	_, err := v6File.extract()
	if err == nil {
		t.Errorf("Expected error trying to read non-existent v6 route file")
	}
	if !strings.Contains(err.Error(), errStrFrag) {
		t.Errorf("Unable to find %q in error string %q", errStrFrag, err.Error())
	}
}

// TestGetAllDefaultRoutesFailNoV4RouteFile 验证 IPv4 路由文件缺失时 getAllDefaultRoutes 返回错误。
func TestGetAllDefaultRoutesFailNoV4RouteFile(t *testing.T) {
	defer func() { v4File.name = ipv4RouteFile }()

	// Should not occur, as caller ensures we have IPv4 route file, but being thorough
	v4File.name = "no-such-file"
	errStrFrag := "no such file"
	_, err := getAllDefaultRoutes()
	if err == nil {
		t.Errorf("Expected error trying to read non-existent v4 route file")
	}
	if !strings.Contains(err.Error(), errStrFrag) {
		t.Errorf("Unable to find %q in error string %q", errStrFrag, err.Error())
	}
}

// TestGetAllDefaultRoutes 验证合并 IPv4/IPv6 路由表后的默认路由集合：
// 无默认路由时报错，仅 v4、仅 v6、双栈分别返回对应路由。
func TestGetAllDefaultRoutes(t *testing.T) {
	testCases := []struct {
		tcase      string
		v4Info     string
		v6Info     string
		count      int
		expected   []Route
		errStrFrag string
	}{
		{"no routes", noInternetConnection, v6noDefaultRoutes, 0, nil, "no default routes"},
		{"only v4 route", gatewayfirst, v6noDefaultRoutes, 1, routeV4, ""},
		{"only v6 route", noInternetConnection, v6gatewayfirst, 1, routeV6, ""},
		{"v4 and v6 routes", gatewayfirst, v6gatewayfirst, 2, bothRoutes, ""},
	}
	defer func() {
		v4File.name = ipv4RouteFile
		v6File.name = ipv6RouteFile
	}()

	for _, tc := range testCases {
		routeFile, err := makeRouteFile(tc.v4Info, t)
		if routeFile != nil {
			defer os.Remove(routeFile.Name())
		}
		if err != nil {
			t.Errorf("case[%s]: test setup failure for IPv4 route file: %v", tc.tcase, err)
		}
		v4File.name = routeFile.Name()
		v6routeFile, err := makeRouteFile(tc.v6Info, t)
		if v6routeFile != nil {
			defer os.Remove(v6routeFile.Name())
		}
		if err != nil {
			t.Errorf("case[%s]: test setup failure for IPv6 route file: %v", tc.tcase, err)
		}
		v6File.name = v6routeFile.Name()

		routes, err := getAllDefaultRoutes()
		if err != nil {
			if !strings.Contains(err.Error(), tc.errStrFrag) {
				t.Errorf("case[%s]: Error string %q does not contain %q", tc.tcase, err, tc.errStrFrag)
			}
		} else if tc.errStrFrag != "" {
			t.Errorf("case[%s]: Error %q expected, but not seen", tc.tcase, tc.errStrFrag)
		} else {
			if tc.count != len(routes) {
				t.Errorf("case[%s]: expected %d routes, have %v", tc.tcase, tc.count, routes)
			}
			for i, expected := range tc.expected {
				if !expected.Gateway.Equal(routes[i].Gateway) {
					t.Errorf("case[%s]: at %d expected %v, got %v .err : %v", tc.tcase, i, tc.expected, routes, err)
				}
				zeroIP := net.IPv4zero
				if expected.Family == familyIPv6 {
					zeroIP = net.IPv6zero
				}
				if !routes[i].Destination.Equal(zeroIP) {
					t.Errorf("case[%s}: at %d destination is not for default route (not %v)", tc.tcase, i, zeroIP)
				}
			}
		}
	}
}
