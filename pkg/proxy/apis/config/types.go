/*
Copyright 2015 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfig "k8s.io/component-base/config"
	logsapi "k8s.io/component-base/logs/api/v1"
)

// KubeProxyLinuxConfiguration 包含 Kubernetes 代理服务器与 Linux 平台相关、
// 但不专属于某个具体后端（如 iptables、ipvs、nftables）的配置细节。
type KubeProxyLinuxConfiguration struct {
	// conntrack 包含与 conntrack 相关的配置项。
	Conntrack KubeProxyConntrackConfiguration
	// masqueradeAll 指示 kube-proxy 对发往 Service cluster IP 的所有流量执行 SNAT。
	// 某些 CNI 插件可能需要开启此选项。
	MasqueradeAll bool
	// oomScoreAdj 是 kube-proxy 进程的 oom-score-adj 值。取值范围必须在 [-1000, 1000] 内。
	OOMScoreAdj *int32
}

// KubeProxyWindowsConfiguration 包含 Kubernetes 代理服务器与 Windows 平台相关、
// 但不专属于某个具体后端的配置细节。
type KubeProxyWindowsConfiguration struct {
	// runAsService 为 true 时，启用与 Windows 服务控制管理器 API 的集成。
	RunAsService bool
}

// KubeProxyIPTablesConfiguration 包含 Kubernetes 代理服务器与 iptables 相关的配置细节。
type KubeProxyIPTablesConfiguration struct {
	// masqueradeBit 是在 iptables 或 ipvs 代理模式下用于 SNAT 的 iptables fwmark 位。
	// 取值范围必须在 [0, 31] 内。
	MasqueradeBit *int32
	// localhostNodePorts 为 false 时，指示 kube-proxy 禁用允许通过 localhost 访问
	// NodePort Service 的旧有行为。（仅适用于 iptables 模式和 IPv4；其他代理模式或
	// IPv6 下永远不允许 localhost NodePort。）
	LocalhostNodePorts *bool
}

// KubeProxyIPVSConfiguration 包含 Kubernetes 代理服务器与 ipvs 相关的配置细节。
type KubeProxyIPVSConfiguration struct {
	// scheduler 是要使用的 IPVS 调度器。
	Scheduler string
	// excludeCIDRs 是 ipvs proxier 在清理 ipvs Service 时不应触碰的 CIDR 列表。
	ExcludeCIDRs []string
	// strictARP 用于配置 arp_ignore 和 arp_announce，以避免从 kube-ipvs0 接口
	// 应答 ARP 查询。
	StrictARP bool
	// tcpTimeout 是空闲 IPVS TCP 会话使用的超时值。默认值为 0，表示保留系统当前的
	// 超时值。
	TCPTimeout metav1.Duration
	// tcpFinTimeout 是收到 FIN 后 IPVS TCP 会话使用的超时值。默认值为 0，表示保留
	// 系统当前的超时值。
	TCPFinTimeout metav1.Duration
	// udpTimeout 是 IPVS UDP 报文使用的超时值。默认值为 0，表示保留系统当前的
	// 超时值。
	UDPTimeout metav1.Duration
}

// KubeProxyNFTablesConfiguration 包含 Kubernetes 代理服务器与 nftables 相关的配置细节。
type KubeProxyNFTablesConfiguration struct {
	// masqueradeBit 是在 nftables 代理模式下用于 SNAT 的 iptables fwmark 位。
	// 取值范围必须在 [0, 31] 内。
	MasqueradeBit *int32
}

// KubeProxyConntrackConfiguration 包含 Kubernetes 代理服务器 的 conntrack 设置。
type KubeProxyConntrackConfiguration struct {
	// maxPerCore 是每个 CPU 核心可跟踪的 NAT 连接数上限（0 表示保持现有限制并忽略 min）。
	MaxPerCore *int32
	// min 是要分配的连接跟踪记录的最小数量，不受 maxPerCore 影响
	// （将 maxPerCore 设为 0 则保持现有限制）。
	Min *int32
	// tcpEstablishedTimeout 是空闲 TCP 连接保持打开的时间（例如 '2s'）。
	// 设置时必须大于 0。
	TCPEstablishedTimeout *metav1.Duration
	// tcpCloseWaitTimeout 是处于 CLOSE_WAIT 状态的空闲 conntrack 条目在 conntrack
	// 表中保留的时间（例如 '60s'）。设置时必须大于 0。
	TCPCloseWaitTimeout *metav1.Duration
	// tcpBeLiberal 为 true 时，kube-proxy 会将 TCP 连接的 conntrack 配置为宽松模式，
	// 序列号超出窗口的数据包不会被标记为 INVALID。
	TCPBeLiberal bool
	// udpTimeout 是处于 UNREPLIED 状态的空闲 UDP conntrack 条目在 conntrack 表中
	// 保留的时间（例如 '30s'）。设置时必须大于 0。
	UDPTimeout metav1.Duration
	// udpStreamTimeout 是处于 ASSURED 状态的空闲 UDP conntrack 条目在 conntrack 表中
	// 保留的时间（例如 '300s'）。设置时必须大于 0。
	UDPStreamTimeout metav1.Duration
}

// KubeProxyWinkernelConfiguration 包含 Kubernetes 代理服务器 的 Windows/HNS 设置。
type KubeProxyWinkernelConfiguration struct {
	// networkName 是 kube-proxy 用于创建 endpoint 和 policy 的网络名称。
	NetworkName string
	// sourceVip 是负载均衡时用于 NAT 的源 VIP endpoint 的 IP 地址。
	SourceVip string
	// enableDSR 指示 kube-proxy 创建 HNS policy 时是否启用 DSR。
	EnableDSR bool
	// rootHnsEndpointName 是挂载到 root 网络命名空间 l2bridge 上的 hnsendpoint 名称。
	RootHnsEndpointName string
	// forwardHealthCheckVip 表示在 Windows 上为健康检查端口转发 Service VIP。
	ForwardHealthCheckVip bool
}

// DetectLocalConfiguration 包含与 DetectLocalMode 选项相关的可选设置。
type DetectLocalConfiguration struct {
	// bridgeInterface 是网桥接口名称。当 DetectLocalMode 设为 LocalModeBridgeInterface 时，
	// kube-proxy 会将来自该网桥的流量视为本地流量。
	BridgeInterface string
	// clusterCIDRs 是集群中 Pod 的 CIDR 范围列表（支持双栈）。当 DetectLocalMode 设为
	// LocalModeClusterCIDR 时，kube-proxy 会将源 IP 落在任一 CIDR 范围内的流量视为本地流量。
	ClusterCIDRs []string
	// interfaceNamePrefix 是接口名称前缀。当 DetectLocalMode 设为 LocalModeInterfaceNamePrefix 时，
	// kube-proxy 会将来自名称以该前缀开头的任意接口的流量视为本地流量。
	InterfaceNamePrefix string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeProxyConfiguration 包含配置 Kubernetes 代理服务器 所需的全部内容。
type KubeProxyConfiguration struct {
	metav1.TypeMeta

	// linux 包含与 Linux 相关的配置项。
	Linux KubeProxyLinuxConfiguration

	// windows 包含与 Windows 相关的配置项。
	Windows KubeProxyWindowsConfiguration

	// featureGates 是功能名称到布尔值的映射，用于启用或禁用 alpha/实验性功能。
	FeatureGates map[string]bool

	// clientConnection 指定代理服务器与 apiserver 通信时使用的 kubeconfig 文件
	// 及客户端连接设置。
	ClientConnection componentbaseconfig.ClientConnectionConfiguration
	// logging 指定日志选项。
	// 更多信息请参阅 [Logs Options](https://github.com/kubernetes/component-base/blob/master/logs/options.go)。
	Logging logsapi.LoggingConfiguration

	// hostnameOverride 非空时，将作为 kube-proxy 所在 Node 的名称。未设置时，
	// 假定节点名称与节点的 hostname 相同。
	HostnameOverride string
	// bindAddress 用于覆盖 kube-proxy 所认为的节点主 IP。注意该名称是历史遗留，
	// kube-proxy 实际上并不会在该 IP 上监听任何 socket。
	BindAddress string
	// healthzBindAddress 是健康检查 server 监听的 IP 地址和端口，默认为
	// "0.0.0.0:10256"（bindAddress 未设置或为 IPv4 时），或 "[::]:10256"（bindAddress 为 IPv6 时）。
	HealthzBindAddress string
	// metricsBindAddress 是 metrics server 监听的 IP 地址和端口，默认为
	// "127.0.0.1:10249"（bindAddress 未设置或为 IPv4 时），或 "[::1]:10249"（bindAddress 为 IPv6 时）。
	// （设置为 "0.0.0.0:10249" / "[::]:10249" 可监听所有接口。）
	MetricsBindAddress string
	// bindAddressHardFail 为 true 时，指示 kube-proxy 将端口绑定失败视为致命错误并退出。
	BindAddressHardFail bool
	// enableProfiling 通过 /debug/pprof 处理器的 Web 接口启用性能分析。
	// 性能分析 handler 由 metrics server 处理。
	EnableProfiling bool
	// showHiddenMetricsForVersion 是要显示隐藏指标所针对的版本。
	ShowHiddenMetricsForVersion string

	// mode 指定要使用的代理模式。
	Mode ProxyMode
	// iptables 包含与 iptables 相关的配置项。
	IPTables KubeProxyIPTablesConfiguration
	// ipvs 包含与 ipvs 相关的配置项。
	IPVS KubeProxyIPVSConfiguration
	// winkernel 包含与 winkernel 相关的配置项。
	Winkernel KubeProxyWinkernelConfiguration
	// nftables 包含与 nftables 相关的配置项。
	NFTables KubeProxyNFTablesConfiguration

	// detectLocalMode 决定检测本地流量所使用的模式，默认为 LocalModeClusterCIDR。
	DetectLocalMode LocalMode
	// detectLocal 包含与 DetectLocalMode 相关的可选配置设置。
	DetectLocal DetectLocalConfiguration

	// nodePortAddresses 是 CIDR 范围和/或可展开为 CIDR 范围的关键字的列表。
	// 只有列表中覆盖的节点 IP 才能访问 NodePort Service。
	// 支持的关键字：'primary'（Node 对象的主 IPv4 和/或 IPv6 地址）、
	// 'localhost'（127.0.0.0/8 和 ::1/128）以及 'all'（0.0.0.0/0 和 ::/0）。
	// 列表中可以包含有效关键字与 CIDR 的任意组合。
	//
	// 在 loopback IP 上提供 NodePort 仅在 iptables 模式下受支持（仅限 IPv4，
	// 参见 iptables.localhostNodePorts）；在 nftables 模式下仅对 TCP 受支持，
	// 且需要启用 KubeProxyNFTablesLocalhostNodePorts feature gate，并在列表中
	// 显式包含 loopback（例如 'localhost'）。
	//
	// 未设置时，iptables 和 ipvs 模式默认为 'all'，nftables 模式默认为 'primary'。
	NodePortAddresses []string

	// syncPeriod 是一个时间间隔（例如 '5s'、'1m'、'2h22m'），表示执行各种重新同步
	// 和清理操作的频率。必须大于 0。
	SyncPeriod metav1.Duration
	// minSyncPeriod 是 proxier 规则重新同步之间的最小周期（例如 '5s'、'1m'、'2h22m'）。
	// 值为 0 表示每次 Service 或 EndpointSlice 变更都会立即触发 proxier 重新同步。
	MinSyncPeriod metav1.Duration
	// configSyncPeriod 是从 apiserver 刷新配置的频率。必须大于 0。
	ConfigSyncPeriod metav1.Duration
}

// ProxyMode 表示 Kubernetes 代理服务器 使用的代理模式。
//
// Linux 平台上有三种代理模式：`iptables`、`ipvs` 和 `nftables`。
// Windows 平台上只有一种代理模式：`kernelspace`。
//
// 如果未指定代理模式，将使用默认模式（目前 Linux 上为 `iptables`，Windows 上为
// `kernelspace`）。如果选定的代理模式无法使用（例如内核支持不足、缺少用户态组件等），
// kube-proxy 将报错退出。
type ProxyMode string

const (
	ProxyModeIPTables    ProxyMode = "iptables"
	ProxyModeIPVS        ProxyMode = "ipvs"
	ProxyModeNFTables    ProxyMode = "nftables"
	ProxyModeKernelspace ProxyMode = "kernelspace"
)

// Set 实现 pflag.Value 接口，将字符串值设置为 ProxyMode。
func (m *ProxyMode) Set(s string) error {
	*m = ProxyMode(s)
	return nil
}

// String 实现 pflag.Value 接口，返回 ProxyMode 的字符串表示；接收者为 nil 时返回空串。
func (m *ProxyMode) String() string {
	if m != nil {
		return string(*m)
	}
	return ""
}

// Type 实现 pflag.Value 接口，返回该标志值的类型名。
func (m *ProxyMode) Type() string {
	return "ProxyMode"
}

// LocalMode 表示从节点检测本地流量的模式。
type LocalMode string

// LocalMode 当前支持的模式。
const (
	LocalModeClusterCIDR         LocalMode = "ClusterCIDR"
	LocalModeNodeCIDR            LocalMode = "NodeCIDR"
	LocalModeBridgeInterface     LocalMode = "BridgeInterface"
	LocalModeInterfaceNamePrefix LocalMode = "InterfaceNamePrefix"
)

// Set 实现 pflag.Value 接口，将字符串值设置为 LocalMode。
func (m *LocalMode) Set(s string) error {
	*m = LocalMode(s)
	return nil
}

// String 实现 pflag.Value 接口，返回 LocalMode 的字符串表示；接收者为 nil 时返回空串。
func (m *LocalMode) String() string {
	if m != nil {
		return string(*m)
	}
	return ""
}

// Type 实现 pflag.Value 接口，返回该标志值的类型名。
func (m *LocalMode) Type() string {
	return "LocalMode"
}

// NodePortAddresses 关键字是 NodePortAddresses 的符号值，会被解析为具体的 CIDR。
// 它们可以相互组合，也可以与字面量的 CIDR 组合使用。
const (
	// NodePortAddressesPrimary 解析为节点各 IP 族的主 IP。
	NodePortAddressesPrimary string = "primary"
	// NodePortAddressesLocalhost 解析为 loopback CIDR。
	NodePortAddressesLocalhost string = "localhost"
	// NodePortAddressesAll 解析为全零 CIDR。
	NodePortAddressesAll string = "all"
)
