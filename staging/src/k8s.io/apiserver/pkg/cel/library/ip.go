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

package library

import (
	"fmt"
	"net/netip"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	apiservercel "k8s.io/apiserver/pkg/cel"
)

// IP provides a CEL function library extension of IP address parsing functions.
//
// ip
//
// Converts a string to an IP address or results in an error if the string is not a valid IP address.
// The IP address must be an IPv4 or IPv6 address.
// IPv4-mapped IPv6 addresses (e.g. ::ffff:1.2.3.4) are not allowed.
// IP addresses with zones (e.g. fe80::1%eth0) are not allowed.
// Leading zeros in IPv4 address octets are not allowed.
//
//	ip(<string>) <IPAddr>
//
// Examples:
//
//	ip('127.0.0.1') // returns an IPv4 address
//	ip('::1') // returns an IPv6 address
//	ip('127.0.0.256') // error
//	ip(':::1') // error
//
// isIP
//
// Returns true if a string is a valid IP address.
// The IP address must be an IPv4 or IPv6 address.
// IPv4-mapped IPv6 addresses (e.g. ::ffff:1.2.3.4) are not allowed.
// IP addresses with zones (e.g. fe80::1%eth0) are not allowed.
// Leading zeros in IPv4 address octets are not allowed.
//
//	isIP(<string>) <bool>
//
// Examples:
//
//	isIP('127.0.0.1') // returns true
//	isIP('::1') // returns true
//	isIP('127.0.0.256') // returns false
//	isIP(':::1') // returns false
//
// ip.isCanonical
//
// Returns true if the IP address is in its canonical form.
// There is exactly one canonical form for every IP address, so fields containing
// IPs in canonical form can just be treated as strings when checking for equality or uniqueness.
//
//	ip.isCanonical(<string>) <bool>
//
// Examples:
//
//	ip.isCanonical('127.0.0.1') // returns true; all valid IPv4 addresses are canonical
//	ip.isCanonical('2001:db8::abcd') // returns true
//	ip.isCanonical('2001:DB8::ABCD') // returns false
//	ip.isCanonical('2001:db8::0:0:0:abcd') // returns false
//
// family / isUnspecified / isLoopback / isLinkLocalMulticast / isLinkLocalUnicast / isGlobalUnicast
//
// - family: returns the IP addresses' family (IPv4 or IPv6) as an integer, either '4' or '6'.
//
// - isUnspecified: returns true if the IP address is the unspecified address.
// Either the IPv4 address "0.0.0.0" or the IPv6 address "::".
//
// - isLoopback: returns true if the IP address is the loopback address.
// Either an IPv4 address with a value of 127.x.x.x or an IPv6 address with a value of ::1.
//
// - isLinkLocalMulticast: returns true if the IP address is a link-local multicast address.
// Either an IPv4 address with a value of 224.0.0.x or an IPv6 address in the network ff00::/8.
//
// - isLinkLocalUnicast: returns true if the IP address is a link-local unicast address.
// Either an IPv4 address with a value of 169.254.x.x or an IPv6 address in the network fe80::/10.
//
// - isGlobalUnicast: returns true if the IP address is a global unicast address.
// Either an IPv4 address that is not zero or 255.255.255.255 or an IPv6 address that is not a link-local unicast, loopback or multicast address.
//
// Examples:
//
//	ip('127.0.0.1').family() // returns '4”
//	ip('::1').family() // returns '6'
//	ip('127.0.0.1').family() == 4 // returns true
//	ip('::1').family() == 6 // returns true
//	ip('0.0.0.0').isUnspecified() // returns true
//	ip('127.0.0.1').isUnspecified() // returns false
//	ip('::').isUnspecified() // returns true
//	ip('::1').isUnspecified() // returns false
//	ip('127.0.0.1').isLoopback() // returns true
//	ip('192.168.0.1').isLoopback() // returns false
//	ip('::1').isLoopback() // returns true
//	ip('2001:db8::abcd').isLoopback() // returns false
//	ip('224.0.0.1').isLinkLocalMulticast() // returns true
//	ip('224.0.1.1').isLinkLocalMulticast() // returns false
//	ip('ff02::1').isLinkLocalMulticast() // returns true
//	ip('fd00::1').isLinkLocalMulticast() // returns false
//	ip('169.254.169.254').isLinkLocalUnicast() // returns true
//	ip('192.168.0.1').isLinkLocalUnicast() // returns false
//	ip('fe80::1').isLinkLocalUnicast() // returns true
//	ip('fd80::1').isLinkLocalUnicast() // returns false
//	ip('192.168.0.1').isGlobalUnicast() // returns true
//	ip('255.255.255.255').isGlobalUnicast() // returns false
//	ip('2001:db8::abcd').isGlobalUnicast() // returns true
//	ip('ff00::1').isGlobalUnicast() // returns false
//
// 实现说明：所有入口最终都经过 parseIPAddr，以保证错误信息与拒绝规则
// （zone、IPv4-mapped）在整个库里完全一致，避免同一字符串在不同函数下
// 得到不同判定，从而产生绕过校验的策略。
func IP() cel.EnvOption {
	return cel.Lib(ipLib)
}

// ipLib 是无状态单例：库的声明是常量，多个 CEL 环境可安全共享。
var ipLib = &ip{}

type ip struct{}

// LibraryName 返回库的稳定名称，用于 CEL 环境的库去重与诊断。
func (*ip) LibraryName() string {
	return "kubernetes.net.ip"
}

// declarations 暴露函数声明，供 apiserver 的 CEL 类型检查/成本估算复用。
func (*ip) declarations() map[string][]cel.FunctionOpt {
	return ipLibraryDecls
}

// Types 返回该库引入的抽象类型，目前只有 IPAddr。
func (*ip) Types() []*cel.Type {
	return []*cel.Type{apiservercel.IPType}
}

// ipLibraryDecls 把 CEL 函数名映射到其重载；成员函数使用 MemberOverload，
// 以便写成 ip('...').family() 形式。重载名必须在环境内唯一。
var ipLibraryDecls = map[string][]cel.FunctionOpt{
	"ip": {
		cel.Overload("string_to_ip", []*cel.Type{cel.StringType}, apiservercel.IPType,
			cel.UnaryBinding(stringToIP)),
	},
	"family": {
		cel.MemberOverload("ip_family", []*cel.Type{apiservercel.IPType}, cel.IntType,
			cel.UnaryBinding(family)),
	},
	"ip.isCanonical": {
		cel.Overload("ip_is_canonical", []*cel.Type{cel.StringType}, cel.BoolType,
			cel.UnaryBinding(ipIsCanonical)),
	},
	"isUnspecified": {
		cel.MemberOverload("ip_is_unspecified", []*cel.Type{apiservercel.IPType}, cel.BoolType,
			cel.UnaryBinding(isUnspecified)),
	},
	"isLoopback": {
		cel.MemberOverload("ip_is_loopback", []*cel.Type{apiservercel.IPType}, cel.BoolType,
			cel.UnaryBinding(isLoopback)),
	},
	"isLinkLocalMulticast": {
		cel.MemberOverload("ip_is_link_local_multicast", []*cel.Type{apiservercel.IPType}, cel.BoolType,
			cel.UnaryBinding(isLinkLocalMulticast)),
	},
	"isLinkLocalUnicast": {
		cel.MemberOverload("ip_is_link_local_unicast", []*cel.Type{apiservercel.IPType}, cel.BoolType,
			cel.UnaryBinding(isLinkLocalUnicast)),
	},
	"isGlobalUnicast": {
		cel.MemberOverload("ip_is_global_unicast", []*cel.Type{apiservercel.IPType}, cel.BoolType,
			cel.UnaryBinding(isGlobalUnicast)),
	},
	"isIP": {
		cel.Overload("is_ip", []*cel.Type{cel.StringType}, cel.BoolType,
			cel.UnaryBinding(isIP)),
	},
	"string": {
		cel.Overload("ip_to_string", []*cel.Type{apiservercel.IPType}, cel.StringType,
			cel.UnaryBinding(ipToString)),
	},
}

// CompileOptions 注册 IP 类型与全部函数重载；编译期即可完成类型检查与重载选择。
func (*ip) CompileOptions() []cel.EnvOption {
	options := []cel.EnvOption{cel.Types(apiservercel.IPType)}
	for name, overloads := range ipLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 该库不需要运行期钩子，返回空列表以满足 cel.Library 接口。
func (*ip) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// stringToIP 实现 ip(<string>)：解析失败返回 CEL 错误值，使表达式求值失败
// 而不是返回零值 IP，避免策略把错误输入当作合法地址。
func stringToIP(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	addr, err := parseIPAddr(s)
	if err != nil {
		// Don't add context, we control the error message already.
		return types.NewErr("%v", err)
	}

	return apiservercel.IP{
		Addr: addr,
	}
}

// ipToString 实现 IPAddr 到字符串的转换；输出总是规范形式。
func ipToString(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.String(ip.Addr.String())
}

// family 返回地址族（4 或 6）。由于 parseIPAddr 已排除 IPv4-mapped，
// 这里不会出现既有 Is4 又 Is6 的歧义。
func family(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	switch {
	case ip.Addr.Is4():
		return types.Int(4)
	case ip.Addr.Is6():
		return types.Int(6)
	default:
		return types.NewErr("IP address %q is not an IPv4 or IPv6 address", ip.Addr.String())
	}
}

// ipIsCanonical 判断字符串是否已是规范形式；用于需要按字符串比较/去重的场景。
func ipIsCanonical(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	addr, err := parseIPAddr(s)
	if err != nil {
		// Don't add context, we control the error message already.
		return types.NewErr("%v", err)
	}

	// Addr.String() always returns the canonical form of the IP address.
	// Therefore comparing this with the original string representation
	// will tell us if the IP address is in its canonical form.
	return types.Bool(addr.String() == s)
}

// isIP 判断字符串是否为合法且被允许的 IP；非法输入返回 false 而非错误。
func isIP(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	_, err := parseIPAddr(s)
	return types.Bool(err == nil)
}

// isUnspecified 判断是否为未指定地址（0.0.0.0 或 ::）。
func isUnspecified(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(ip.Addr.IsUnspecified())
}

// isLoopback 判断是否为回环地址（127.0.0.0/8 或 ::1）。
func isLoopback(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(ip.Addr.IsLoopback())
}

// isLinkLocalMulticast 判断是否为链路本地组播（224.0.0.0/24 或 ff02::/16）。
func isLinkLocalMulticast(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(ip.Addr.IsLinkLocalMulticast())
}

// isLinkLocalUnicast 判断是否为链路本地单播（169.254.0.0/16 或 fe80::/10），
// 常用于识别云厂商元数据地址。
func isLinkLocalUnicast(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(ip.Addr.IsLinkLocalUnicast())
}

// isGlobalUnicast 判断是否为全局单播地址。
func isGlobalUnicast(arg ref.Val) ref.Val {
	ip, ok := arg.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(ip.Addr.IsGlobalUnicast())
}

// parseIPAddr parses a string into an IP address.
// We use this function to parse IP addresses in the CEL library
// so that we can share the common logic of rejecting IP addresses
// that contain zones or are IPv4-mapped IPv6 addresses.
func parseIPAddr(raw string) (netip.Addr, error) {
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("IP Address %q parse error during conversion from string: %v", raw, err)
	}

	// zone 使地址依赖主机本地接口，跨节点比较没有意义，且会让规范形式不稳定。
	if addr.Zone() != "" {
		return netip.Addr{}, fmt.Errorf("IP address %q with zone value is not allowed", raw)
	}

	// IPv4-mapped IPv6 与对应 IPv4 在语义上相同，允许两者共存会绕过去重/白名单校验。
	if addr.Is4In6() {
		return netip.Addr{}, fmt.Errorf("IPv4-mapped IPv6 address %q is not allowed", raw)
	}

	return addr, nil
}
