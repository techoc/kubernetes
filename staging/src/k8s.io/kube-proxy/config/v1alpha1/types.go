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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
	logsapi "k8s.io/component-base/logs/api/v1"
)

// KubeProxyIPTablesConfiguration 收拢 iptables 模式特有的调优参数。
// 模式相关字段独立分组，避免顶层配置被其他转发实现的选项塞满。
type KubeProxyIPTablesConfiguration struct {
	// MasqueradeBit 是 iptables 或 ipvs 模式下用于标记 SNAT 流量的 fwmark 位，取值范围 [0, 31]。
	// 指针用于区分未设置与显式的合法值 0，未设置时由默认值逻辑填充。
	MasqueradeBit *int32 `json:"masqueradeBit"`
	// MasqueradeAll 为 true 时，对发往 Service ClusterIP 的所有流量执行 SNAT。
	// 某些 CNI 需要借此修正回包路由；代价是丢失原始源 IP。
	MasqueradeAll bool `json:"masqueradeAll"`
	// LocalhostNodePorts 为 false 时禁止通过本机回环地址访问 NodePort。
	// 该字段仅适用于 iptables 模式和 IPv4；nftables 的回环 NodePort 由功能门和 NodePortAddresses 控制。
	// 指针用于区分未设置与显式 false，避免布尔零值被误判为用户选择。
	LocalhostNodePorts *bool `json:"localhostNodePorts"`
	// SyncPeriod 是重新同步和清理操作的间隔（如 '5s'、'1m'、'2h22m'），必须大于 0。
	// 全量同步用于纠正丢失事件或外部规则改动；间隔为 0 会造成持续全量同步。
	SyncPeriod metav1.Duration `json:"syncPeriod"`
	// MinSyncPeriod 是 iptables 规则重新同步的最小间隔（如 '5s'、'1m'、'2h22m'）。
	// 0 表示不节流，每次 Service 或 EndpointSlice 变更都会立即重新同步。
	MinSyncPeriod metav1.Duration `json:"minSyncPeriod"`
}

// KubeProxyIPVSConfiguration 收拢 IPVS 模式特有的会话超时、调度器和同步参数。
type KubeProxyIPVSConfiguration struct {
	// SyncPeriod 是重新同步和清理操作的间隔（如 '5s'、'1m'、'2h22m'），必须大于 0。
	SyncPeriod metav1.Duration `json:"syncPeriod"`
	// MinSyncPeriod 是 IPVS 规则重新同步的最小间隔（如 '5s'、'1m'、'2h22m'）。
	// 0 表示每次 Service 或 EndpointSlice 变更都立即重新同步，不进行节流。
	MinSyncPeriod metav1.Duration `json:"minSyncPeriod"`
	// Scheduler 是 IPVS 使用的调度算法（如 rr、lc），由内核负责后端负载分发。
	Scheduler string `json:"scheduler"`
	// ExcludeCIDRs 列出清理 IPVS 服务时必须跳过的网段，避免误删其他组件创建的规则。
	ExcludeCIDRs []string `json:"excludeCIDRs"`
	// StrictARP 配置 arp_ignore 与 arp_announce，防止 kube-ipvs0 接口应答不该接收的 ARP 请求。
	StrictARP bool `json:"strictARP"`
	// TCPTimeout 是空闲 IPVS TCP 会话的超时；0 表示保留系统当前值，而非立即超时。
	TCPTimeout metav1.Duration `json:"tcpTimeout"`
	// TCPFinTimeout 是 IPVS TCP 会话收到 FIN 后的超时；0 表示保留系统当前值。
	TCPFinTimeout metav1.Duration `json:"tcpFinTimeout"`
	// UDPTimeout 是 IPVS UDP 包的超时；0 表示保留系统当前值。
	UDPTimeout metav1.Duration `json:"udpTimeout"`
}

// KubeProxyNFTablesConfiguration 是 nftables 模式的 SNAT 与同步配置。
type KubeProxyNFTablesConfiguration struct {
	// MasqueradeBit 是 nftables 模式下用于标记 SNAT 流量的 fwmark 位，取值范围 [0, 31]。
	// 指针用于区分未设置与显式的合法值 0。
	MasqueradeBit *int32 `json:"masqueradeBit"`
	// MasqueradeAll 为 true 时，对发往 Service ClusterIP 的所有流量执行 SNAT。
	// 某些 CNI 需要借此修正回包路由；代价是丢失原始源 IP。
	MasqueradeAll bool `json:"masqueradeAll"`
	// SyncPeriod 是重新同步和清理操作的间隔（如 '5s'、'1m'、'2h22m'），必须大于 0。
	SyncPeriod metav1.Duration `json:"syncPeriod"`
	// MinSyncPeriod 是 nftables 规则重新同步的最小间隔（如 '5s'、'1m'、'2h22m'）。
	// 0 表示每次 Service 或 EndpointSlice 变更都立即重新同步，不进行节流。
	MinSyncPeriod metav1.Duration `json:"minSyncPeriod"`
}

// KubeProxyConntrackConfiguration 控制内核 conntrack 表的容量与超时。
// 表满会丢弃新连接，因此这些参数直接影响服务可用性；指针字段用于区分未设置与显式零值。
type KubeProxyConntrackConfiguration struct {
	// MaxPerCore 是每个 CPU 核允许跟踪的最大 NAT 连接数。
	// 0 表示保留系统上限并忽略 Min，避免两个容量来源互相覆盖。
	MaxPerCore *int32 `json:"maxPerCore"`
	// Min 是 conntrack 表的最小容量，不受 MaxPerCore 按核缩放结果影响。
	// MaxPerCore 为 0 时该字段不生效。
	Min *int32 `json:"min"`
	// TCPEstablishedTimeout 是空闲 TCP 连接的保留时间（如 '2s'）；设置时必须大于 0。
	// 缩短超时能更快释放失效条目，也可能提前回收仍被间接使用的长连接。
	TCPEstablishedTimeout *metav1.Duration `json:"tcpEstablishedTimeout"`
	// TCPCloseWaitTimeout 是 CLOSE_WAIT 状态条目在 conntrack 表中的保留时间（如 '60s'）；设置时必须大于 0。
	TCPCloseWaitTimeout *metav1.Duration `json:"tcpCloseWaitTimeout"`
	// TCPBeLiberal 为 true 时启用 TCP 宽松模式，窗口外序列号的报文不会被标记为 INVALID。
	// 这可减少乱序或重传报文被误丢，但也会放宽对异常报文的检查。
	TCPBeLiberal bool `json:"tcpBeLiberal"`
	// UDPTimeout 是 UNREPLIED 状态 UDP 条目的保留时间（如 '30s'）；设置时必须大于 0。
	UDPTimeout metav1.Duration `json:"udpTimeout"`
	// UDPStreamTimeout 是 ASSURED 状态 UDP 条目的保留时间（如 '300s'）；设置时必须大于 0。
	UDPStreamTimeout metav1.Duration `json:"udpStreamTimeout"`
}

// KubeProxyWinkernelConfiguration 是 Windows kernelspace 模式的 HNS 配置，仅用于 Windows 节点。
type KubeProxyWinkernelConfiguration struct {
	// NetworkName 是创建 HNS endpoint 与 policy 时使用的网络名称。
	NetworkName string `json:"networkName"`
	// SourceVip 是负载均衡时用于 NAT 的源 VIP 端点 IP 地址。
	SourceVip string `json:"sourceVip"`
	// EnableDSR 为 true 时让 HNS policy 启用 DSR，由后端直接向客户端返回流量。
	// 这可减少转发跳数，但依赖底层网络支持。
	EnableDSR bool `json:"enableDSR"`
	// RootHnsEndpointName 是根网络命名空间连接到 l2bridge 所使用的 HNS endpoint 名称。
	RootHnsEndpointName string `json:"rootHnsEndpointName"`
	// ForwardHealthCheckVip 为 true 时转发健康检查端口上的 Service VIP 流量。
	ForwardHealthCheckVip bool `json:"forwardHealthCheckVip"`
}

// DetectLocalConfiguration 保存 DetectLocalMode 所需的本地流量判定参数。
// 网桥名与接口名前缀对应不同模式，由 DetectLocalMode 决定读取哪个字段。
type DetectLocalConfiguration struct {
	// BridgeInterface 是 LocalModeBridgeInterface 模式使用的网桥名；来自该网桥的流量视为本地流量。
	BridgeInterface string `json:"bridgeInterface"`
	// InterfaceNamePrefix 是 LocalModeInterfaceNamePrefix 模式使用的接口名前缀；匹配前缀的流量视为本地流量。
	InterfaceNamePrefix string `json:"interfaceNamePrefix"`
}

// 指令 +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object 让生成器为
// 该类型实现 runtime.Object 接口所需的 DeepCopyObject 方法。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeProxyConfiguration 是 kube-proxy 的用户可见配置格式。
// 命令行、文件和 ConfigMap 配置最终统一到该结构；内部转换与默认值处理由组件配置加载流程负责。
type KubeProxyConfiguration struct {
	metav1.TypeMeta `json:""`

	// FeatureGates 按名称启用或禁用 alpha、实验性功能。
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// ClientConnection 是访问 apiserver 所用的 kubeconfig 与客户端连接参数，复用 component-base 语义。
	ClientConnection componentbaseconfigv1alpha1.ClientConnectionConfiguration `json:"clientConnection"`
	// Logging 是日志配置；详见 [Logs Options](https://github.com/kubernetes/component-base/blob/master/logs/options.go)。
	Logging logsapi.LoggingConfiguration `json:"logging,omitempty"`

	// HostnameOverride 非空时用作 kube-proxy 所属 Node 的名称，否则使用节点主机名。
	HostnameOverride string `json:"hostnameOverride"`
	// BindAddress 覆盖 kube-proxy 推断的节点主 IP；该历史字段名不代表实际套接字绑定地址。
	BindAddress string `json:"bindAddress"`
	// HealthzBindAddress 是健康检查服务器的监听地址。
	// 默认在 bindAddress 未设置或为 IPv4 时使用 "0.0.0.0:10256"，为 IPv6 时使用 "[::]:10256"。
	HealthzBindAddress string `json:"healthzBindAddress"`
	// MetricsBindAddress 是指标服务器的监听地址。
	// 默认在 bindAddress 未设置或为 IPv4 时使用 "127.0.0.1:10249"，为 IPv6 时使用 "[::1]:10249"；
	// 设置为 "0.0.0.0:10249" 或 "[::]:10249" 可监听所有接口，但会扩大暴露面。
	MetricsBindAddress string `json:"metricsBindAddress"`
	// BindAddressHardFail 为 true 时，将辅助服务器端口绑定失败视为致命错误并退出。
	BindAddressHardFail bool `json:"bindAddressHardFail"`
	// EnableProfiling 在指标服务器的 /debug/pprof 处理器上启用性能分析。
	EnableProfiling bool `json:"enableProfiling"`
	// ShowHiddenMetricsForVersion 指定临时显示已隐藏指标的版本，用于指标下线过渡。
	ShowHiddenMetricsForVersion string `json:"showHiddenMetricsForVersion"`

	// Mode 指定转发模式；同一时间只能启用一种代理实现。
	Mode ProxyMode `json:"mode"`
	// IPTables 保存 iptables 模式参数；未选用该模式时忽略。
	IPTables KubeProxyIPTablesConfiguration `json:"iptables"`
	// IPVS 保存 ipvs 模式参数；未选用该模式时忽略。
	IPVS KubeProxyIPVSConfiguration `json:"ipvs"`
	// NFTables 保存 nftables 模式参数；未选用该模式时忽略。
	NFTables KubeProxyNFTablesConfiguration `json:"nftables"`
	// Winkernel 保存 Windows kernelspace 模式参数；未选用该模式时忽略。
	Winkernel KubeProxyWinkernelConfiguration `json:"winkernel"`

	// DetectLocalMode 指定本地流量判定方式，默认为 ClusterCIDR。
	DetectLocalMode LocalMode `json:"detectLocalMode"`
	// DetectLocal 保存 DetectLocalMode 所需的可选参数。
	DetectLocal DetectLocalConfiguration `json:"detectLocal"`
	// ClusterCIDR 是集群 Pod CIDR；双栈集群可用逗号分隔两个 CIDR。
	// 仅当 DetectLocalMode 为 ClusterCIDR 时，来源 IP 落在该范围的流量才被视为本地流量。
	ClusterCIDR string `json:"clusterCIDR"`

	// NodePortAddresses 限定可访问 NodePort 的节点 IP，可填 CIDR、关键字或两者的组合。
	// 支持 `primary`（节点主 IP）、`localhost`（127.0.0.0/8 和 ::1/128）和
	// `all`（0.0.0.0/0 和 ::/0）；未设置时 iptables、ipvs 默认 `all`，nftables 默认 `primary`。
	// iptables 仅支持 IPv4 回环 NodePort；nftables 仅在功能门开启且显式包含回环地址时支持 TCP。
	NodePortAddresses []string `json:"nodePortAddresses"`

	// OOMScoreAdj 指定 kube-proxy 进程的 oom-score-adj，取值必须在 [-1000, 1000] 内。
	// 使用指针区分未设置与显式设置 0，避免将两者混为一谈。
	OOMScoreAdj *int32 `json:"oomScoreAdj"`
	// Conntrack 保存各数据面模式通用的连接跟踪参数。
	Conntrack KubeProxyConntrackConfiguration `json:"conntrack"`
	// ConfigSyncPeriod 指定从 apiserver 刷新配置的周期，必须大于 0。
	// 周期过短会持续增加 API 请求；设为 0 则没有有效的刷新节奏。
	ConfigSyncPeriod metav1.Duration `json:"configSyncPeriod"`

	// PortRange 曾用于配置 userspace 代理，现已不用。
	// 保留字段仅为兼容旧配置文件，避免反序列化失败。
	PortRange string `json:"portRange"`

	// WindowsRunAsService 为 true 时启用 Windows 服务控制管理器集成。
	WindowsRunAsService bool `json:"windowsRunAsService,omitempty"`
}

// ProxyMode 指定 kube-proxy 转发模式。
// Linux 支持 iptables、ipvs 和 nftables，Windows 支持 kernelspace。
// 未指定时使用平台默认模式；选定模式不可用时 kube-proxy 会报错退出，不静默回退，以免掩盖配置问题。
type ProxyMode string

// LocalMode 指定从节点识别本地流量的方式。
// 判定结果决定是否跳过 SNAT；误判可能错误转换本机 Pod 流量，因此需匹配实际网络环境。
type LocalMode string
