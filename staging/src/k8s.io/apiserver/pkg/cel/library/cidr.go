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

// CIDR provides a CEL function library extension of CIDR notation parsing functions.
//
// cidr
//
// Converts a string in CIDR notation to a network address representation or results in an error if the string is not a valid CIDR notation.
// The CIDR must be an IPv4 or IPv6 subnet address with a mask.
// Leading zeros in IPv4 address octets are not allowed.
// IPv4-mapped IPv6 addresses (e.g. ::ffff:1.2.3.4/24) are not allowed.
//
//	cidr(<string>) <CIDR>
//
// Examples:
//
//	cidr('192.168.0.0/16') // returns an IPv4 address with a CIDR mask
//	cidr('::1/128') // returns an IPv6 address with a CIDR mask
//	cidr('192.168.0.0/33') // error
//	cidr('::1/129') // error
//	cidr('192.168.0.1/16') // error, because there are non-0 bits after the prefix
//
// isCIDR
//
// Returns true if a string is a valid CIDR notation respresentation of a subnet with mask.
// The CIDR must be an IPv4 or IPv6 subnet address with a mask.
// Leading zeros in IPv4 address octets are not allowed.
// IPv4-mapped IPv6 addresses (e.g. ::ffff:1.2.3.4/24) are not allowed.
//
//	isCIDR(<string>) <bool>
//
// Examples:
//
//	isCIDR('192.168.0.0/16') // returns true
//	isCIDR('::1/128') // returns true
//	isCIDR('192.168.0.0/33') // returns false
//	isCIDR('::1/129') // returns false
//
// containsIP / containerCIDR / ip / masked / prefixLength
//
// - containsIP: Returns true if a the CIDR contains the given IP address.
// The IP address must be an IPv4 or IPv6 address.
// May take either a string or IP address as an argument.
//
// - containsCIDR: Returns true if a the CIDR contains the given CIDR.
// The CIDR must be an IPv4 or IPv6 subnet address with a mask.
// May take either a string or CIDR as an argument.
//
// - ip: Returns the IP address representation of the CIDR.
//
// - masked: Returns the CIDR representation of the network address with a masked prefix.
// This can be used to return the canonical form of the CIDR network.
//
// - prefixLength: Returns the prefix length of the CIDR in bits.
// This is the number of bits in the mask.
//
// Examples:
//
//	cidr('192.168.0.0/24').containsIP(ip('192.168.0.1')) // returns true
//	cidr('192.168.0.0/24').containsIP(ip('192.168.1.1')) // returns false
//	cidr('192.168.0.0/24').containsIP('192.168.0.1') // returns true
//	cidr('192.168.0.0/24').containsIP('192.168.1.1') // returns false
//	cidr('192.168.0.0/16').containsCIDR(cidr('192.168.10.0/24')) // returns true
//	cidr('192.168.1.0/24').containsCIDR(cidr('192.168.2.0/24')) // returns false
//	cidr('192.168.0.0/16').containsCIDR('192.168.10.0/24') // returns true
//	cidr('192.168.1.0/24').containsCIDR('192.168.2.0/24') // returns false
//	cidr('192.168.0.1/24').ip() // returns ipAddr('192.168.0.1')
//	cidr('192.168.0.1/24').ip().family() // returns '4'
//	cidr('::1/128').ip() // returns ipAddr('::1')
//	cidr('::1/128').ip().family() // returns '6'
//	cidr('192.168.0.0/24').masked() // returns cidr('192.168.0.0/24')
//	cidr('192.168.0.1/24').masked() // returns cidr('192.168.0.0/24')
//	cidr('192.168.0.0/24') == cidr('192.168.0.0/24').masked() // returns true, CIDR was already in canonical format
//	cidr('192.168.0.1/24') == cidr('192.168.0.1/24').masked() // returns false, CIDR was not in canonical format
//	cidr('192.168.0.0/16').prefixLength() // returns 16
//	cidr('::1/128').prefixLength() // returns 128
//
// CIDR 返回一个可装入 CEL 环境的库选项。解析刻意比 net.ParseCIDR 更严格：
// 拒绝 IPv4 映射的 IPv6 地址与掩码后非零的网络地址，保证 cidr() 的结果
// 是规范化的网络地址，避免“同一子网多种写法”导致策略比较失效。
func CIDR() cel.EnvOption {
	return cel.Lib(cidrsLib)
}

// cidrsLib 是无状态的库单例；所有重载都通过函数表按需注册。
var cidrsLib = &cidrs{}

// cidrs 实现 cel.Library 接口，仅用于挂载 CIDR/IP 类型与函数声明。
type cidrs struct{}

// LibraryName 用于 CEL 环境的库标识与调试；改名会被视为不同库。
func (*cidrs) LibraryName() string {
	return "kubernetes.net.cidr"
}

// declarations 返回库内所有函数重载，供 CompileOptions 批量注册。
func (*cidrs) declarations() map[string][]cel.FunctionOpt {
	return cidrLibraryDecls
}

// Types 声明库引入的抽象类型，使 CEL 类型检查认识 CIDR 与 IP。
func (*cidrs) Types() []*cel.Type {
	return []*cel.Type{apiservercel.CIDRType, apiservercel.IPType}
}

// cidrLibraryDecls 把 CEL 函数名映射到重载列表。string 重载放在前面，
// 便于调用方直接用字符串字面量而无需先构造 CIDR/IP。
var cidrLibraryDecls = map[string][]cel.FunctionOpt{
	"cidr": {
		cel.Overload("string_to_cidr", []*cel.Type{cel.StringType}, apiservercel.CIDRType,
			cel.UnaryBinding(stringToCIDR)),
	},
	"containsIP": {
		cel.MemberOverload("cidr_contains_ip_string", []*cel.Type{apiservercel.CIDRType, cel.StringType}, cel.BoolType,
			cel.BinaryBinding(cidrContainsIPString)),
		cel.MemberOverload("cidr_contains_ip_ip", []*cel.Type{apiservercel.CIDRType, apiservercel.IPType}, cel.BoolType,
			cel.BinaryBinding(cidrContainsIP)),
	},
	"containsCIDR": {
		cel.MemberOverload("cidr_contains_cidr_string", []*cel.Type{apiservercel.CIDRType, cel.StringType}, cel.BoolType,
			cel.BinaryBinding(cidrContainsCIDRString)),
		cel.MemberOverload("cidr_contains_cidr", []*cel.Type{apiservercel.CIDRType, apiservercel.CIDRType}, cel.BoolType,
			cel.BinaryBinding(cidrContainsCIDR)),
	},
	"ip": {
		cel.MemberOverload("cidr_ip", []*cel.Type{apiservercel.CIDRType}, apiservercel.IPType,
			cel.UnaryBinding(cidrToIP)),
	},
	"prefixLength": {
		cel.MemberOverload("cidr_prefix_length", []*cel.Type{apiservercel.CIDRType}, cel.IntType,
			cel.UnaryBinding(prefixLength)),
	},
	"masked": {
		cel.MemberOverload("cidr_masked", []*cel.Type{apiservercel.CIDRType}, apiservercel.CIDRType,
			cel.UnaryBinding(masked)),
	},
	"isCIDR": {
		cel.Overload("is_cidr", []*cel.Type{cel.StringType}, cel.BoolType,
			cel.UnaryBinding(isCIDR)),
	},
	"string": {
		cel.Overload("cidr_to_string", []*cel.Type{apiservercel.CIDRType}, cel.StringType,
			cel.UnaryBinding(cidrToString)),
	},
}

// CompileOptions 注册类型与全部函数重载；每个环境只需调用一次。
func (*cidrs) CompileOptions() []cel.EnvOption {
	options := []cel.EnvOption{cel.Types(apiservercel.CIDRType)}
	for name, overloads := range cidrLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 该库不需要运行期钩子（无状态函数），返回空列表。
func (*cidrs) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// stringToCIDR 实现 cidr(string)：解析失败以 CEL 错误返回，使表达式在该点
// 短路/按策略的失败处理，而不是得到静默的空值。
func stringToCIDR(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	net, err := parseCIDR(s)
	if err != nil {
		return types.NewErr("network address parse error during conversion from string: %v", err)
	}

	return apiservercel.CIDR{
		Prefix: net,
	}
}

// cidrToString 实现 CIDR 的 string() 转换，输出带掩码的规范字符串。
func cidrToString(arg ref.Val) ref.Val {
	cidr, ok := arg.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.String(cidr.Prefix.String())
}

// cidrContainsIPString 是 containsIP(string) 重载：先解析 IP 再做包含判断。
func cidrContainsIPString(arg ref.Val, other ref.Val) ref.Val {
	return cidrContainsIP(arg, stringToIP(other))
}

// cidrContainsCIDRString 是 containsCIDR(string) 重载：先解析子网再判断包含。
func cidrContainsCIDRString(arg ref.Val, other ref.Val) ref.Val {
	return cidrContainsCIDR(arg, stringToCIDR(other))
}

// cidrContainsIP 判断 CIDR 是否包含某个 IP。参数顺序错误时返回重载错误，
// 避免把用户手误当成 false。
func cidrContainsIP(arg ref.Val, other ref.Val) ref.Val {
	cidr, ok := arg.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}

	ip, ok := other.(apiservercel.IP)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Bool(cidr.Contains(ip.Addr))
}

// cidrContainsCIDR 判断包含关系：既要网段重叠，还要被包含方的前缀长度不短于
// 容器（即容器范围更宽），否则 CIDR 只覆盖对方的一部分。
func cidrContainsCIDR(arg ref.Val, other ref.Val) ref.Val {
	cidr, ok := arg.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	containsCIDR, ok := other.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}

	return types.Bool(cidr.Overlaps(containsCIDR.Prefix) && cidr.Prefix.Bits() <= containsCIDR.Prefix.Bits())
}

// prefixLength 返回掩码位数（0-32/128），供策略做数值比较。
func prefixLength(arg ref.Val) ref.Val {
	cidr, ok := arg.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return types.Int(cidr.Prefix.Bits())
}

// isCIDR 只做校验并返回布尔值，不产生错误；用于策略中的条件判断。
func isCIDR(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	_, err := parseCIDR(s)
	return types.Bool(err == nil)
}

// cidrToIP 取子网的网络地址作为 IP 返回；注意不是任意主机地址。
func cidrToIP(arg ref.Val) ref.Val {
	cidr, ok := arg.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	return apiservercel.IP{
		Addr: cidr.Prefix.Addr(),
	}
}

// masked 返回规范化（清除主机位）后的 CIDR，可用于把用户输入统一成网络地址。
func masked(arg ref.Val) ref.Val {
	cidr, ok := arg.(apiservercel.CIDR)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	maskedCIDR := cidr.Prefix.Masked()
	return apiservercel.CIDR{
		Prefix: maskedCIDR,
	}
}

// parseCIDR parses a string into an CIDR.
// We use this function to parse CIDR notation in the CEL library
// so that we can share the common logic of rejecting strings
// that IPv4-mapped IPv6 addresses or contain non-zero bits after the mask.
// netip.ParsePrefix 本身已拒绝掩码后仍有主机位的写法；这里额外拒绝
// IPv4-mapped IPv6，避免同一地址族出现两种表示。
func parseCIDR(raw string) (netip.Prefix, error) {
	net, err := netip.ParsePrefix(raw)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("network address parse error during conversion from string: %v", err)
	}

	if net.Addr().Is4In6() {
		return netip.Prefix{}, fmt.Errorf("IPv4-mapped IPv6 address %q is not allowed", raw)
	}

	return net, nil
}
