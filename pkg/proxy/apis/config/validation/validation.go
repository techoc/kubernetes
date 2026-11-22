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

package validation

import (
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	componentbaseconfig "k8s.io/component-base/config"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	netutils "k8s.io/utils/net"
)

// Validate 校验 kube-proxy 的完整配置。
// 所有校验项集中在此执行，以便一次性列出配置中的所有问题。
func Validate(config *kubeproxyconfig.KubeProxyConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}

	newPath := field.NewPath("KubeProxyConfiguration")

	// 特性门控先深拷贝再应用配置，避免校验过程污染全局默认门控。
	effectiveFeatures := utilfeature.DefaultFeatureGate.DeepCopy()
	if err := effectiveFeatures.SetFromMap(config.FeatureGates); err != nil {
		allErrs = append(allErrs, field.Invalid(newPath.Child("featureGates"), config.FeatureGates, err.Error()))
	}

	allErrs = append(allErrs, validateKubeProxyIPTablesConfiguration(config.IPTables, newPath.Child("KubeProxyIPTablesConfiguration"))...)
	// IPVS 与 nftables 配置只在对应代理模式下生效，因此按需校验，不相关的模式不报错。
	switch config.Mode {
	case kubeproxyconfig.ProxyModeIPVS:
		allErrs = append(allErrs, validateKubeProxyIPVSConfiguration(config.IPVS, newPath.Child("KubeProxyIPVSConfiguration"))...)
	case kubeproxyconfig.ProxyModeNFTables:
		allErrs = append(allErrs, validateKubeProxyNFTablesConfiguration(config.NFTables, newPath.Child("KubeProxyNFTablesConfiguration"))...)
	}
	allErrs = append(allErrs, validateKubeProxyLinuxConfiguration(config.Linux, newPath.Child("KubeProxyLinuxConfiguration"))...)
	allErrs = append(allErrs, validateProxyMode(config.Mode, newPath.Child("Mode"))...)
	allErrs = append(allErrs, validateClientConnectionConfiguration(config.ClientConnection, newPath.Child("ClientConnection"))...)

	// 同步周期必须为正，且最小同步周期不能大于常规同步周期，否则节流逻辑会被反转。
	if config.ConfigSyncPeriod.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(newPath.Child("ConfigSyncPeriod"), config.ConfigSyncPeriod, "must be greater than 0"))
	}
	if config.SyncPeriod.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(newPath.Child("SyncPeriod"), config.SyncPeriod, "must be greater than 0"))
	}
	if config.MinSyncPeriod.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(newPath.Child("MinSyncPeriod"), config.MinSyncPeriod, "must be greater than or equal to 0"))
	}
	if config.MinSyncPeriod.Duration > config.SyncPeriod.Duration {
		allErrs = append(allErrs, field.Invalid(newPath.Child("SyncPeriod"), config.MinSyncPeriod, fmt.Sprintf("must be greater than or equal to %s", newPath.Child("MinSyncPeriod").String())))
	}

	// 绑定地址必须是可解析的 IP，代理需要据此监听并处理流量。
	if netutils.ParseIPSloppy(config.BindAddress) == nil {
		allErrs = append(allErrs, field.Invalid(newPath.Child("BindAddress"), config.BindAddress, "not a valid textual representation of an IP address"))
	}

	if config.HealthzBindAddress != "" {
		allErrs = append(allErrs, validateHostPort(config.HealthzBindAddress, newPath.Child("HealthzBindAddress"))...)
	}
	allErrs = append(allErrs, validateHostPort(config.MetricsBindAddress, newPath.Child("MetricsBindAddress"))...)

	allErrs = append(allErrs, validateKubeProxyNodePortAddress(config.NodePortAddresses, newPath.Child("NodePortAddresses"))...)
	allErrs = append(allErrs, validateShowHiddenMetricsVersion(config.ShowHiddenMetricsForVersion, newPath.Child("ShowHiddenMetricsForVersion"))...)

	allErrs = append(allErrs, validateDetectLocalMode(config.DetectLocalMode, newPath.Child("DetectLocalMode"))...)
	allErrs = append(allErrs, validateDetectLocalConfiguration(config.DetectLocalMode, config.DetectLocal, newPath.Child("DetectLocalConfiguration"))...)
	allErrs = append(allErrs, logsapi.Validate(&config.Logging, effectiveFeatures, newPath.Child("logging"))...)

	return allErrs
}

// validateKubeProxyIPTablesConfiguration 校验 iptables 模式的专有配置。
func validateKubeProxyIPTablesConfiguration(config kubeproxyconfig.KubeProxyIPTablesConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// MasqueradeBit 最终用于 fwmark（32 位整数），超出 [0, 31] 的位移无法生效。
	if config.MasqueradeBit != nil && (*config.MasqueradeBit < 0 || *config.MasqueradeBit > 31) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("MasqueradeBit"), config.MasqueradeBit, "must be within the range [0, 31]"))
	}
	return allErrs
}

// validateKubeProxyIPVSConfiguration 校验 IPVS 模式的专有配置：连接超时与排除网段。
func validateKubeProxyIPVSConfiguration(config kubeproxyconfig.KubeProxyIPVSConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateIPVSTimeout(config, fldPath)...)
	allErrs = append(allErrs, validateIPVSExcludeCIDRs(config.ExcludeCIDRs, fldPath.Child("ExcludeCidrs"))...)

	return allErrs
}

// validateKubeProxyNFTablesConfiguration 校验 nftables 模式的专有配置。
func validateKubeProxyNFTablesConfiguration(config kubeproxyconfig.KubeProxyNFTablesConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// nftables 同样把 MasqueradeBit 作为 fwmark 的位偏移，范围必须落在 [0, 31]。
	if config.MasqueradeBit != nil && (*config.MasqueradeBit < 0 || *config.MasqueradeBit > 31) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("MasqueradeBit"), config.MasqueradeBit, "must be within the range [0, 31]"))
	}

	return allErrs
}

// validateKubeProxyLinuxConfiguration 校验仅在 Linux 上生效的 kube-proxy 配置。
func validateKubeProxyLinuxConfiguration(config kubeproxyconfig.KubeProxyLinuxConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateKubeProxyConntrackConfiguration(config.Conntrack, fldPath.Child("KubeProxyConntrackConfiguration"))...)

	if config.OOMScoreAdj != nil && (*config.OOMScoreAdj < -1000 || *config.OOMScoreAdj > 1000) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("OOMScoreAdj"), *config.OOMScoreAdj, "must be within the range [-1000, 1000]"))
	}

	return allErrs
}

// validateKubeProxyConntrackConfiguration 校验 conntrack 表项上限与各协议超时时间。
func validateKubeProxyConntrackConfiguration(config kubeproxyconfig.KubeProxyConntrackConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 负值会破坏按 CPU 核数计算表项上限的逻辑，因此上限只能是非负数。
	if config.MaxPerCore != nil && *config.MaxPerCore < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("MaxPerCore"), config.MaxPerCore, "must be greater than or equal to 0"))
	}

	if config.Min != nil && *config.Min < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("Min"), config.Min, "must be greater than or equal to 0"))
	}

	// TCPEstablishedTimeout 有默认值，不会为 nil，可以直接读取 Duration。
	if config.TCPEstablishedTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("TCPEstablishedTimeout"), config.TCPEstablishedTimeout, "must be greater than or equal to 0"))
	}

	// TCPCloseWaitTimeout 有默认值，不会为 nil，可以直接读取 Duration。
	if config.TCPCloseWaitTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("TCPCloseWaitTimeout"), config.TCPCloseWaitTimeout, "must be greater than or equal to 0"))
	}

	if config.UDPTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("UDPTimeout"), config.UDPTimeout, "must be greater than or equal to 0"))
	}

	if config.UDPStreamTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("UDPStreamTimeout"), config.UDPStreamTimeout, "must be greater than or equal to 0"))
	}

	return allErrs
}

// validateProxyMode 按运行平台分派校验：Windows 与 Linux 支持的代理模式集合不同。
func validateProxyMode(mode kubeproxyconfig.ProxyMode, fldPath *field.Path) field.ErrorList {
	if runtime.GOOS == "windows" {
		return validateProxyModeWindows(mode, fldPath)
	}

	return validateProxyModeLinux(mode, fldPath)
}

// validateProxyModeLinux 校验 Linux 平台允许的代理模式。
func validateProxyModeLinux(mode kubeproxyconfig.ProxyMode, fldPath *field.Path) field.ErrorList {
	// 空字符串表示使用默认模式（当前为 iptables）。
	validModes := sets.New[string](
		string(kubeproxyconfig.ProxyModeIPTables),
		string(kubeproxyconfig.ProxyModeNFTables),
	)

	// IPVS 受特性门控保护，只有显式开启后才允许配置。
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeProxyIPVS) {
		validModes.Insert(string(kubeproxyconfig.ProxyModeIPVS))
	}

	if mode == "" || validModes.Has(string(mode)) {
		return nil
	}

	errMsg := fmt.Sprintf("must be %s or blank (blank means the best-available proxy [currently iptables])", strings.Join(sets.List(validModes), ", "))
	return field.ErrorList{field.Invalid(fldPath.Child("ProxyMode"), string(mode), errMsg)}
}

// validateProxyModeWindows 校验 Windows 平台允许的代理模式。
// Windows 目前仅支持 kernelspace 模式，空字符串表示使用默认模式。
func validateProxyModeWindows(mode kubeproxyconfig.ProxyMode, fldPath *field.Path) field.ErrorList {
	validModes := sets.New[string](
		string(kubeproxyconfig.ProxyModeKernelspace),
	)

	if mode == "" || validModes.Has(string(mode)) {
		return nil
	}

	errMsg := fmt.Sprintf("must be %s or blank (blank means the most-available proxy [currently 'kernelspace'])", strings.Join(sets.List(validModes), ", "))
	return field.ErrorList{field.Invalid(fldPath.Child("ProxyMode"), string(mode), errMsg)}
}

// validateDetectLocalMode 校验本地端点探测模式是否为受支持的值。
func validateDetectLocalMode(mode kubeproxyconfig.LocalMode, fldPath *field.Path) field.ErrorList {
	validModes := []string{
		string(kubeproxyconfig.LocalModeClusterCIDR),
		string(kubeproxyconfig.LocalModeNodeCIDR),
		string(kubeproxyconfig.LocalModeBridgeInterface),
		string(kubeproxyconfig.LocalModeInterfaceNamePrefix),
		"",
	}

	if sets.New(validModes...).Has(string(mode)) {
		return nil
	}

	return field.ErrorList{field.NotSupported(fldPath, string(mode), validModes)}
}

// validateClientConnectionConfiguration 校验客户端连接配置，例如 API 请求的突发上限。
func validateClientConnectionConfiguration(config componentbaseconfig.ClientConnectionConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(config.Burst), fldPath.Child("Burst"))...)
	return allErrs
}

// validateHostPort 校验 "IP:端口" 形式的监听地址。
// IP 与端口分别解析，确保二者都是合法值，而不是只满足整体格式。
func validateHostPort(input string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	hostIP, port, err := net.SplitHostPort(input)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, input, "must be IP:port"))
		return allErrs
	}

	if ip := netutils.ParseIPSloppy(hostIP); ip == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, hostIP, "must be a valid IP"))
	}

	if p, err := strconv.Atoi(port); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, port, "must be a valid port"))
	} else if p < 1 || p > 65535 {
		allErrs = append(allErrs, field.Invalid(fldPath, port, "must be a valid port"))
	}

	return allErrs
}

// validateKubeProxyNodePortAddress 校验 NodePort 地址列表。
// 除 CIDR 外只允许三个预定义关键字，且关键字不能重复出现。
func validateKubeProxyNodePortAddress(nodePortAddresses []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 用集合记录已出现的关键字，重复配置没有意义，需要单独报错。
	seenKeywords := sets.New[string]()
	for i := range nodePortAddresses {
		switch nodePortAddresses[i] {
		case kubeproxyconfig.NodePortAddressesPrimary,
			kubeproxyconfig.NodePortAddressesLocalhost,
			kubeproxyconfig.NodePortAddressesAll:
			if seenKeywords.Has(nodePortAddresses[i]) {
				allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), nodePortAddresses[i]))
			}
			seenKeywords.Insert(nodePortAddresses[i])
			// 关键字已通过白名单校验，无需再按 CIDR 解析。
			continue
		}

		if _, _, err := netutils.ParseCIDRSloppy(nodePortAddresses[i]); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), nodePortAddresses[i], "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'"))
		}
	}

	return allErrs
}

// validateIPVSTimeout 校验 IPVS 各协议的超时时间不能为负。
func validateIPVSTimeout(config kubeproxyconfig.KubeProxyIPVSConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if config.TCPTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("TCPTimeout"), config.TCPTimeout, "must be greater than or equal to 0"))
	}

	if config.TCPFinTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("TCPFinTimeout"), config.TCPFinTimeout, "must be greater than or equal to 0"))
	}

	if config.UDPTimeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("UDPTimeout"), config.UDPTimeout, "must be greater than or equal to 0"))
	}

	return allErrs
}

// validateIPVSExcludeCIDRs 校验 IPVS 排除网段列表中的每一项都是合法 CIDR。
func validateIPVSExcludeCIDRs(excludeCIDRs []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for i := range excludeCIDRs {
		if _, _, err := netutils.ParseCIDRSloppy(excludeCIDRs[i]); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), excludeCIDRs[i], "must be a valid CIDR"))
		}
	}
	return allErrs
}

// validateShowHiddenMetricsVersion 校验隐藏指标版本号能否被 metrics 组件接受。
func validateShowHiddenMetricsVersion(version string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	errs := metrics.ValidateShowHiddenMetricsVersion(version)
	for _, e := range errs {
		allErrs = append(allErrs, field.Invalid(fldPath, version, e.Error()))
	}

	return allErrs
}

// validateInterface 校验本地探测模式所需的接口名非空。
func validateInterface(iface string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(iface) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, iface, "must not be empty"))
	}
	return allErrs
}

// validateDualStackCIDRStrings 校验单栈或双栈 CIDR 列表：最多两项，且每项都是合法地址段。
func validateDualStackCIDRStrings(cidrStrings []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch {
	case len(cidrStrings) == 0:
		allErrs = append(allErrs, field.Invalid(fldPath, cidrStrings, "must contain at least one CIDR"))
	case len(cidrStrings) > 2:
		allErrs = append(allErrs, field.Invalid(fldPath, cidrStrings, "must be a either a single CIDR or dual-stack pair of CIDRs (e.g. [10.100.0.0/16, fde4:8dba:82e1::/48]"))
	default:
		for i, cidrString := range cidrStrings {
			if _, _, err := netutils.ParseCIDRSloppy(cidrString); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(i), cidrString, "must be a valid CIDR block (e.g. 10.100.0.0/16 or fde4:8dba:82e1::/48)"))
			}
		}
		if len(cidrStrings) == 2 {
			ifDualStack, err := netutils.IsDualStackCIDRStrings(cidrStrings)
			// 只有两个 CIDR 都合法且不属于不同 IP 家族时，才报告非双栈错误。
			if err == nil && !ifDualStack {
				allErrs = append(allErrs, field.Invalid(fldPath, cidrStrings, "must be a either a single CIDR or dual-stack pair of CIDRs (e.g. [10.100.0.0/16, fde4:8dba:82e1::/48]"))
			}
		}
	}
	return allErrs
}

// validateDetectLocalConfiguration 根据所选的本地探测模式，校验其实际使用的配置字段。
// 与当前模式无关的字段不校验，避免无关配置阻塞启动。
func validateDetectLocalConfiguration(mode kubeproxyconfig.LocalMode, config kubeproxyconfig.DetectLocalConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch mode {
	case kubeproxyconfig.LocalModeBridgeInterface:
		allErrs = append(allErrs, validateInterface(config.BridgeInterface, fldPath.Child("InterfaceName"))...)
	case kubeproxyconfig.LocalModeInterfaceNamePrefix:
		allErrs = append(allErrs, validateInterface(config.InterfaceNamePrefix, fldPath.Child("InterfacePrefix"))...)
	case kubeproxyconfig.LocalModeClusterCIDR:
		if len(config.ClusterCIDRs) > 0 {
			allErrs = append(allErrs, validateDualStackCIDRStrings(config.ClusterCIDRs, fldPath.Child("ClusterCIDRs"))...)
		}
	}
	return allErrs
}
