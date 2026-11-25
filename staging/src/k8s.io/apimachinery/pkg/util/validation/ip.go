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

package validation

import (
	"fmt"
	"net"
	"net/netip"
	"slices"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// parseIP 解析并校验 IP，返回解析结果与全部错误。
//
// 先用宽松解析器保证历史格式可读，再在严格模式下用 net/netip 检查
// 前导零与 IPv4-mapped IPv6 这两种有歧义的写法。
func parseIP(fldPath *field.Path, value string, strictValidation bool) (net.IP, field.ErrorList) {
	var allErrors field.ErrorList

	ip := netutils.ParseIPSloppy(value)
	if ip == nil {
		allErrors = append(allErrors, field.Invalid(fldPath, value, "must be a valid IP address, (e.g. 10.9.8.7 or 2001:db8::ffff)"))
		return nil, allErrors
	}

	if strictValidation {
		addr, err := netip.ParseAddr(value)
		if err != nil {
			//如果 netutils.ParseIPSloppy 能解析而 netip.ParseAddr 不能，
			//说明存在非法的前导零。
			// 宽松解析器接受 "010.0.0.1"，严格解析器拒绝：差异只可能来自前导零。
			allErrors = append(allErrors, field.Invalid(fldPath, value, "must not have leading 0s"))
		}
		if addr.Is4In6() {
			// IPv4-mapped IPv6 可能被不同软件分别当作 IPv4/IPv6，存在安全风险。
			allErrors = append(allErrors, field.Invalid(fldPath, value, "must not be an IPv4-mapped IPv6 address"))
		}
	}

	return ip, allErrors
}

// IsValidIPForLegacyField 验证参数是否为严格 IP 校验出现前的 "legacy" API 字段的有效 IP。
// 它允许非规范形式，例如 "FE80:0abc:0:0:0:0:0:0/64" 而不是 "fe80:abc::/64"。
//
// strictValidation 为 false 时，还允许某些无效或有歧义的格式：
//
//  1. IPv4 各段允许前导零（如 "010.002.003.004"）。
//     历史上 net.ParseIP（以及后来的 netutils.ParseIPSloppy）会忽略 IPv4 前导零，
//     但多数基于 libc 的软件将其视为八进制，导致同一字符串可能被解释为不同 IP，
//     存在安全风险。当前 net.ParseIP 和 netip.ParseAddr 都会拒绝带前导零的输入。
//
//  2. 允许 IPv4-mapped IPv6 地址（如 "::ffff:1.2.3.4"）。不同软件可能分别按 IPv4 或 IPv6 解释，
//     也存在歧义。net.ParseIP 和 netip.ParseAddr 都允许这种格式，
//     但 Kubernetes 没有将 IPv4 地址表示为 IPv4-mapped IPv6 的用例。
//
// 验证现有字段更新时，也可以传入旧对象中的 CIDR 列表，
// 使新对象中再次出现的这些值即使无效也被接受。
// 此函数只应用于历史上按此方式校验的现有字段；
// 除非 StrictIPCIDRValidation 特性门关闭，否则 strictValidation 应为 true。
func IsValidIPForLegacyField(fldPath *field.Path, value string, strictValidation bool, validOldIPs []string) field.ErrorList {
	if slices.Contains(validOldIPs, value) {
		// 旧对象中已存在的值必须继续接受，否则更新会因未修改的字段而失败。
		return nil
	}
	_, allErrors := parseIP(fldPath, value, strictValidation)
	return allErrors.WithOrigin("format=ip-sloppy")
}

// IsValidIP 按 Kubernetes 当前标准验证参数是否为有效 IP 地址。
//
// 严格校验还要求规范形式：例如 IPv6 必须使用 RFC 5952 的压缩写法。
func IsValidIP(fldPath *field.Path, value string) field.ErrorList {
	ip, allErrors := parseIP(fldPath, value, true)
	if len(allErrors) != 0 {
		return allErrors.WithOrigin("format=ip-strict")
	}

	if value != ip.String() {
		allErrors = append(allErrors, field.Invalid(fldPath, value, fmt.Sprintf("must be in canonical form (%q)", ip.String())))
	}
	return allErrors.WithOrigin("format=ip-strict")
}

// GetWarningsForIP 返回非标准格式 IP 的警告，
// 只应与使用 IsValidIPForLegacyField() 校验的字段配合使用。
func GetWarningsForIP(fldPath *field.Path, value string) []string {
	ip := netutils.ParseIPSloppy(value)
	if ip == nil {
		//nolint:logcheck // 不应执行到这里。
		klog.ErrorS(nil, "GetWarningsForIP called on value that was not validated with IsValidIPForLegacyField", "field", fldPath, "value", value)
		return nil
	}

	addr, _ := netip.ParseAddr(value)
	if !addr.IsValid() || addr.Is4In6() {
		//这覆盖两种情况：前导零（ParseIPSloppy 接受而 ParseAddr 拒绝），
		//或 IPv4-mapped IPv6（.Is4In6()）；两种情况都应将 net.IP 重新格式化为首选形式。
		return []string{
			fmt.Sprintf("%s: non-standard IP address %q is invalid: use %q", fldPath, value, ip.String()),
		}
	}

	//如果 ParseIPSloppy() 和 ParseAddr() 都接受，则值有效，但可能不是规范形式。
	if addr.Is6() && addr.String() != value {
		return []string{
			fmt.Sprintf("%s: IPv6 address %q should be in RFC 5952 canonical format (%q)", fldPath, value, addr.String()),
		}
	}

	return nil
}

// parseCIDR 解析并校验 CIDR，逻辑与 parseIP 对应：
// 宽松解析保证可读，严格模式额外检查前导零、IPv4-mapped IPv6，
// 以及"前缀之后仍有置位"的接口地址写法。
func parseCIDR(fldPath *field.Path, value string, strictValidation bool) (*net.IPNet, field.ErrorList) {
	var allErrors field.ErrorList

	_, ipnet, err := netutils.ParseCIDRSloppy(value)
	if err != nil {
		allErrors = append(allErrors, field.Invalid(fldPath, value, "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)"))
		return nil, allErrors
	}

	if strictValidation {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			//如果 netutils.ParseCIDRSloppy 能解析而 netip.ParsePrefix 不能，
			//说明 IP 部分或前缀长度存在非法前导零。
			allErrors = append(allErrors, field.Invalid(fldPath, value, "must not have leading 0s in IP or prefix length"))
		} else if prefix.Addr().Is4In6() {
			allErrors = append(allErrors, field.Invalid(fldPath, value, "must not have an IPv4-mapped IPv6 address"))
		} else if prefix.Addr() != prefix.Masked().Addr() {
			// "192.168.1.5/24" 这类接口地址在 subnet 语义下是歧义的。
			allErrors = append(allErrors, field.Invalid(fldPath, value, "must not have bits set beyond the prefix length"))
		}
	}

	return ipnet, allErrors
}

// IsValidCIDRForLegacyField 验证参数是否为严格 IP 校验出现前的 "legacy" API 字段的有效 CIDR。
// 它允许非规范形式，例如 "FE80:0abc:0:0:0:0:0:0/64" 而不是 "fe80:abc::/64"。
//
// strictValidation 为 false 时，还允许某些无效或有歧义的 CIDR 格式：
//
// 1. CIDR 的 IP 部分按 IsValidIPForLegacyField 且 strictValidation=false 的规则解析。
//
//  2. 允许 "subnet"/"mask"（前缀长度后的低位全为零），或 `ip addr` 形式的 "interface address"；
//     严格校验要求必须是 "subnet"/"mask" 形式。
//
// 3. 前缀长度允许前导零。
// 验证现有字段更新时，也可以传入旧对象中的 CIDR 列表，
// 使新对象中再次出现的这些值即使无效也被接受。
// 此函数只应用于历史上按此方式校验的现有字段；
// 除非 StrictIPCIDRValidation 特性门关闭，否则 strictValidation 应为 true。
// 新字段请使用 IsValidCIDR 或 IsValidInterfaceAddress。
func IsValidCIDRForLegacyField(fldPath *field.Path, value string, strictValidation bool, validOldCIDRs []string) field.ErrorList {
	if slices.Contains(validOldCIDRs, value) {
		// 与 IP 相同：旧值豁免，保证既有对象仍可更新。
		return nil
	}

	_, allErrors := parseCIDR(fldPath, value, strictValidation)
	return allErrors
}

// IsValidCIDR 按 Kubernetes 当前标准验证 CIDR，仅接受 "subnet"/"mask" 形式，
// 例如 "192.168.1.0/24"，前缀长度后的位必须全为零。
// `ip addr` 形式请使用 IsValidInterfaceAddress。
func IsValidCIDR(fldPath *field.Path, value string) field.ErrorList {
	ipnet, allErrors := parseCIDR(fldPath, value, true)
	if len(allErrors) != 0 {
		return allErrors
	}

	if value != ipnet.String() {
		allErrors = append(allErrors, field.Invalid(fldPath, value, fmt.Sprintf("must be in canonical form (%q)", ipnet.String())))
	}
	return allErrors
}

// GetWarningsForCIDR 返回非标准格式 CIDR 的警告，
// 只应与使用 IsValidCIDRForLegacyField() 校验的字段配合使用。
func GetWarningsForCIDR(fldPath *field.Path, value string) []string {
	ip, ipnet, err := netutils.ParseCIDRSloppy(value)
	if err != nil {
		//nolint:logcheck // 不应执行到这里。
		klog.ErrorS(err, "GetWarningsForCIDR called on value that was not validated with IsValidCIDRForLegacyField", "field", fldPath, "value", value)
		return nil
	}

	var warnings []string

	//检查前缀长度之后是否有置位。
	if !ip.Equal(ipnet.IP) {
		_, addrlen := ipnet.Mask.Size()
		singleIPCIDR := fmt.Sprintf("%s/%d", ip.String(), addrlen)
		warnings = append(warnings,
			fmt.Sprintf("%s: CIDR value %q is ambiguous in this context (should be %q or %q?)", fldPath, value, ipnet.String(), singleIPCIDR),
		)
	}

	prefix, _ := netip.ParsePrefix(value)
	addr := prefix.Addr()
	if !prefix.IsValid() || addr.Is4In6() {
		//这覆盖两种情况：前导零（ParseCIDRSloppy 接受而 ParsePrefix 拒绝），
		//或 IPv4-mapped IPv6（.Is4In6()）；两种情况都应将 net.IPNet 重新格式化为首选形式。
		warnings = append(warnings,
			fmt.Sprintf("%s: non-standard CIDR value %q is invalid: use %q", fldPath, value, ipnet.String()),
		)
	}

	//如果 ParseCIDRSloppy() 和 ParsePrefix() 都接受，则值有效，但可能不是规范形式。
	//仅在没有其他警告时检查；其他警告也会导致往返失败。
	if len(warnings) == 0 && addr.Is6() && prefix.String() != value {
		warnings = append(warnings,
			fmt.Sprintf("%s: IPv6 CIDR value %q should be in RFC 5952 canonical format (%q)", fldPath, value, prefix.String()),
		)
	}

	return warnings
}

// IsValidInterfaceAddress 验证参数是否为规范形式的 "ifaddr" CIDR，
// 例如 "192.168.1.5/24"，包含完整 IP 和对应子网长度。
// "subnet"/"mask" 形式请使用 IsValidCIDR。
// "192.168.1.0/24").
func IsValidInterfaceAddress(fldPath *field.Path, value string) field.ErrorList {
	var allErrors field.ErrorList
	ip, ipnet, err := netutils.ParseCIDRSloppy(value)
	if err != nil {
		allErrors = append(allErrors, field.Invalid(fldPath, value, "must be a valid address in CIDR form, (e.g. 10.9.8.7/24 or 2001:db8::1/64)"))
		return allErrors
	}

	//value 的规范形式不是 ipnet.String()，因为 ipnet 不包含前缀长度后的位；
	//需要根据 ip 和 ipnet.Mask 自行构造。
	maskSize, _ := ipnet.Mask.Size()
	if netutils.IsIPv4(ip) && maskSize > net.IPv4len*8 {
		// "::ffff:192.168.0.1/120" -> "192.168.0.1/24"
		// 去掉 IPv4-mapped 前缀带来的 96 位。
		maskSize -= (net.IPv6len - net.IPv4len) * 8
	}
	canonical := fmt.Sprintf("%s/%d", ip.String(), maskSize)
	if value != canonical {
		allErrors = append(allErrors, field.Invalid(fldPath, value, fmt.Sprintf("must be in canonical form (%q)", canonical)))
	}
	return allErrors
}
