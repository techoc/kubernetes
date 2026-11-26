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

// IP 提供一组用于解析 IP 地址的 CEL 函数库扩展。
//
// ip
//
// 将字符串转换为 IP 地址；如果字符串不是合法 IP 地址则产生错误。
// IP 地址必须是 IPv4 或 IPv6 地址。
// 不允许 IPv4 映射的 IPv6 地址（例如 ::ffff:1.2.3.4）。
// 不允许带 zone 的 IP 地址（例如 fe80::1%eth0）。
// 不允许 IPv4 地址各段中出现前导零。
//
//	ip(<string>) <IPAddr>
//
// 示例：
//
//	ip('127.0.0.1') // 返回 IPv4 地址
//	ip('::1') // 返回 IPv6 地址
//	ip('127.0.0.256') // 错误
//	ip(':::1') // 错误
//
// isIP
//
// 如果字符串是合法 IP 地址则返回 true。
// IP 地址必须是 IPv4 或 IPv6 地址。
// 不允许 IPv4 映射的 IPv6 地址（例如 ::ffff:1.2.3.4）。
// 不允许带 zone 的 IP 地址（例如 fe80::1%eth0）。
// 不允许 IPv4 地址各段中出现前导零。
//
//	isIP(<string>) <bool>
//
// 示例：
//
//	isIP('127.0.0.1') // 返回 true
//	isIP('::1') // 返回 true
//	isIP('127.0.0.256') // 返回 false
//	isIP(':::1') // 返回 false
//
// ip.isCanonical
//
// 如果 IP 地址处于规范形式则返回 true。
// 每个 IP 地址有且仅有一个规范形式，因此对于以规范形式存储 IP 的字段，
// 在检查相等性或唯一性时可以直接按字符串处理。
//
//	ip.isCanonical(<string>) <bool>
//
// 示例：
//
//	ip.isCanonical('127.0.0.1') // 返回 true；所有合法 IPv4 地址都是规范形式
//	ip.isCanonical('2001:db8::abcd') // 返回 true
//	ip.isCanonical('2001:DB8::ABCD') // 返回 false
//	ip.isCanonical('2001:db8::0:0:0:abcd') // 返回 false
//
// family / isUnspecified / isLoopback / isLinkLocalMulticast / isLinkLocalUnicast / isGlobalUnicast
//
// - family: 以整数形式返回 IP 地址的地址族（IPv4 或 IPv6），取值为 '4' 或 '6'。
//
// - isUnspecified: 如果 IP 地址是未指定地址则返回 true。
// 即 IPv4 地址 "0.0.0.0" 或 IPv6 地址 "::"。
//
// - isLoopback: 如果 IP 地址是回环地址则返回 true。
// 即取值为 127.x.x.x 的 IPv4 地址，或取值为 ::1 的 IPv6 地址。
//
// - isLinkLocalMulticast: 如果 IP 地址是链路本地组播地址则返回 true。
// 即取值为 224.0.0.x 的 IPv4 地址，或位于 ff02::/16 网段内的 IPv6 地址。
// （原英文注释写作 ff00::/8，但实现委托给 netip.Addr.IsLinkLocalMulticast，实际判定 ff02::/16。）
//
// - isLinkLocalUnicast: 如果 IP 地址是链路本地单播地址则返回 true。
// 即取值为 169.254.x.x 的 IPv4 地址，或位于 fe80::/10 网段内的 IPv6 地址。
//
// - isGlobalUnicast: 如果 IP 地址是全局单播地址则返回 true。
// 即非零且非 255.255.255.255 的 IPv4 地址，或非链路本地单播、非回环、非组播的 IPv6 地址。
//
// 示例：
//
//	ip('127.0.0.1').family() // 返回 '4'
//	ip('::1').family() // 返回 '6'
//	ip('127.0.0.1').family() == 4 // 返回 true
//	ip('::1').family() == 6 // 返回 true
//	ip('0.0.0.0').isUnspecified() // 返回 true
//	ip('127.0.0.1').isUnspecified() // 返回 false
//	ip('::').isUnspecified() // 返回 true
//	ip('::1').isUnspecified() // 返回 false
//	ip('127.0.0.1').isLoopback() // 返回 true
//	ip('192.168.0.1').isLoopback() // 返回 false
//	ip('::1').isLoopback() // 返回 true
//	ip('2001:db8::abcd').isLoopback() // 返回 false
//	ip('224.0.0.1').isLinkLocalMulticast() // 返回 true
//	ip('224.0.1.1').isLinkLocalMulticast() // 返回 false
//	ip('ff02::1').isLinkLocalMulticast() // 返回 true
//	ip('fd00::1').isLinkLocalMulticast() // 返回 false
//	ip('169.254.169.254').isLinkLocalUnicast() // 返回 true
//	ip('192.168.0.1').isLinkLocalUnicast() // 返回 false
//	ip('fe80::1').isLinkLocalUnicast() // 返回 true
//	ip('fd80::1').isLinkLocalUnicast() // 返回 false
//	ip('192.168.0.1').isGlobalUnicast() // 返回 true
//	ip('255.255.255.255').isGlobalUnicast() // 返回 false
//	ip('2001:db8::abcd').isGlobalUnicast() // 返回 true
//	ip('ff00::1').isGlobalUnicast() // 返回 false
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
		// 不再叠加上下文，错误信息已由我们自己控制。
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
		// 不再叠加上下文，错误信息已由我们自己控制。
		return types.NewErr("%v", err)
	}

	// Addr.String() 总是返回 IP 地址的规范形式。
	// 因此把它与原始字符串表示比较，
	// 即可判断该 IP 地址是否已处于规范形式。
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

// parseIPAddr 将字符串解析为 IP 地址。
// CEL 库中所有 IP 地址的解析都走这个函数，
// 以便共用同一套拒绝逻辑：拒绝带 zone 的地址，
// 以及 IPv4 映射的 IPv6 地址。
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
