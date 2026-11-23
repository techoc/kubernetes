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

// KubeProxyIPTablesConfiguration contains iptables-related configuration
// details for the Kubernetes proxy server.
//
// KubeProxyIPTablesConfiguration 收拢 iptables 模式特有的调优参数。
// 每种代理模式的内核数据结构和同步路径差别很大，把模式相关字段拆成独立结构，
// 既能让配置文件按模式分组，也能避免顶层结构被所有模式的选项塞满。
type KubeProxyIPTablesConfiguration struct {
	// masqueradeBit is the bit of the iptables fwmark space to use for SNAT if using
	// the iptables or ipvs proxy mode. Values must be within the range [0, 31].
	//
	// 用指针是为了区分「未填写」与「显式填 0」：前者应交给默认值填充逻辑，
	// 后者是合法的位号。fwmark 只有 32 位，所以值域限定在 [0, 31]。
	MasqueradeBit *int32 `json:"masqueradeBit"`
	// masqueradeAll tells kube-proxy to SNAT all traffic sent to Service cluster IPs,
	// when using the iptables or ipvs proxy mode. This may be required with some CNI
	// plugins.
	//
	// 有些 CNI 的路由设置会让发往 ClusterIP 的流量绕过正常的 SNAT 路径，
	// 此时需要强制全量 SNAT 才能回包正确。代价是丢失源 IP，所以默认关闭。
	MasqueradeAll bool `json:"masqueradeAll"`
	// localhostNodePorts, if false, tells kube-proxy to disable the legacy behavior
	// of allowing NodePort services to be accessed via localhost. (Applies only to
	// iptables mode and IPv4; localhost NodePorts are never allowed with other proxy
	// modes or with IPv6.)
	//
	// 这是一次行为变更的开关：过去通过本机回环访问 NodePort 是可行的，
	// 现在默认关掉以缩小暴露面。用指针是为了让「未设置」也能走默认化逻辑，
	// 而不是被布尔零值误判为显式关闭。
	LocalhostNodePorts *bool `json:"localhostNodePorts"`
	// syncPeriod is an interval (e.g. '5s', '1m', '2h22m') indicating how frequently
	// various re-synchronizing and cleanup operations are performed. Must be greater
	// than 0.
	//
	// 全量同步是兜底机制：即使监听事件丢失或规则被外部改动，经过这段时间也会被纠正。
	// 间隔设为 0 会导致每收到一个事件都做全量同步，开销过大，因此要求大于 0。
	SyncPeriod metav1.Duration `json:"syncPeriod"`
	// minSyncPeriod is the minimum period between iptables rule resyncs (e.g. '5s',
	// '1m', '2h22m'). A value of 0 means every Service or EndpointSlice change will
	// result in an immediate iptables resync.
	//
	// 这里限制的是增量同步的最小间隔，用于在高变更频率下给 iptables 更新做节流。
	// 与 SyncPeriod 的分工是：最小值管「事件触发」，同步周期管「定时兜底」。
	// 设为 0 表示不节流，每次变更都立即重写规则，适合追求低延迟但不在乎 CPU 的场景。
	MinSyncPeriod metav1.Duration `json:"minSyncPeriod"`
}

// KubeProxyIPVSConfiguration contains ipvs-related configuration
// details for the Kubernetes proxy server.
//
// KubeProxyIPVSConfiguration 收拢 IPVS 模式特有的选项。IPVS 的内核连接表自带超时和
// 调度算法，因此这里除了同步周期，还有会话超时、调度器等 iptables 模式没有的字段。
type KubeProxyIPVSConfiguration struct {
	// syncPeriod is an interval (e.g. '5s', '1m', '2h22m') indicating how frequently
	// various re-synchronizing and cleanup operations are performed. Must be greater
	// than 0.
	//
	// 同 iptables 模式一样，这是防止规则漂移的全量同步周期；要求大于 0 避免忙循环。
	SyncPeriod metav1.Duration `json:"syncPeriod"`
	// minSyncPeriod is the minimum period between IPVS rule resyncs (e.g. '5s', '1m',
	// '2h22m'). A value of 0 means every Service or EndpointSlice change will result
	// in an immediate IPVS resync.
	//
	// 对变更事件做节流的下限。设为 0 时每次 Service/EndpointSlice 变更都立即重算，
	// 延迟最低但会在频繁变更时产生大量系统调用。
	MinSyncPeriod metav1.Duration `json:"minSyncPeriod"`
	// scheduler is the IPVS scheduler to use
	//
	// 对应 IPVS 的调度算法（如 rr、lc）。交给内核实现负载分发，
	// kube-proxy 只负责把 Service 映射成 IPVS 虚拟服务器。
	Scheduler string `json:"scheduler"`
	// excludeCIDRs is a list of CIDRs which the ipvs proxier should not touch
	// when cleaning up ipvs services.
	//
	// 清理阶段跳过这些网段，避免误删同一节点上由其他组件（如服务网格）创建的 IPVS 规则。
	ExcludeCIDRs []string `json:"excludeCIDRs"`
	// strictARP configures arp_ignore and arp_announce to avoid answering ARP queries
	// from kube-ipvs0 interface
	//
	// 让内核只回答本命 ARP 请求。多用于 VIP 与节点 IP 重叠的部署方式，
	// 否则 kube-ipvs0 上的 VIP 会抢答，导致流量被错误地引到本机。
	StrictARP bool `json:"strictARP"`
	// tcpTimeout is the timeout value used for idle IPVS TCP sessions.
	// The default value is 0, which preserves the current timeout value on the system.
	//
	// 0 表示不覆盖系统当前的超时值，即沿用管理员在内核里设置的值，
	// 而不是把它误当作「立即超时」。
	TCPTimeout metav1.Duration `json:"tcpTimeout"`
	// tcpFinTimeout is the timeout value used for IPVS TCP sessions after receiving a FIN.
	// The default value is 0, which preserves the current timeout value on the system.
	//
	// 同上，0 表示保留系统已有配置。
	TCPFinTimeout metav1.Duration `json:"tcpFinTimeout"`
	// udpTimeout is the timeout value used for IPVS UDP packets.
	// The default value is 0, which preserves the current timeout value on the system.
	//
	// UDP 没有连接状态，这个超时决定内核何时回收伪连接条目；0 仍表示沿用系统值。
	UDPTimeout metav1.Duration `json:"udpTimeout"`
}

// KubeProxyNFTablesConfiguration contains nftables-related configuration
// details for the Kubernetes proxy server.
//
// KubeProxyNFTablesConfiguration 是 nftables 模式的配置。nftables 被视为 iptables 的
// 后继者，因此这里保留了与之相似的 SNAT 和同步周期选项，但不支持全部旧模式的行为
// （例如只有部分场景支持本机 NodePort）。
type KubeProxyNFTablesConfiguration struct {
	// masqueradeBit is the bit of the iptables fwmark space to use for SNAT if using
	// the nftables proxy mode. Values must be within the range [0, 31].
	//
	// 虽然叫 iptables fwmark，但 nftables 沿用了同一套 mark 位来标记需要 SNAT 的流量。
	// 指针类型同样是为了区分未设置和合法的 0。
	MasqueradeBit *int32 `json:"masqueradeBit"`
	// masqueradeAll tells kube-proxy to SNAT all traffic sent to Service cluster IPs,
	// when using the nftables mode. This may be required with some CNI plugins.
	//
	// 与 iptables 模式同名的开关含义一致：为兼容特定 CNI 而强制全量 SNAT，代价是丢源 IP。
	MasqueradeAll bool `json:"masqueradeAll"`
	// syncPeriod is an interval (e.g. '5s', '1m', '2h22m') indicating how frequently
	// various re-synchronizing and cleanup operations are performed. Must be greater
	// than 0.
	//
	// 全量同步的兜底周期，必须大于 0，否则会持续重写规则表。
	SyncPeriod metav1.Duration `json:"syncPeriod"`
	// minSyncPeriod is the minimum period between iptables rule resyncs (e.g. '5s',
	// '1m', '2h22m'). A value of 0 means every Service or EndpointSlice change will
	// result in an immediate iptables resync.
	//
	// 事件触发更新的节流下限；在注释沿用 iptables 措辞，是因为 nftables 实现
	// 复用了相同的同步节奏概念。0 表示不去重、每次变更都立即重写。
	MinSyncPeriod metav1.Duration `json:"minSyncPeriod"`
}

// KubeProxyConntrackConfiguration contains conntrack settings for
// the Kubernetes proxy server.
//
// KubeProxyConntrackConfiguration 控制内核 conntrack 表的容量与超时。kube-proxy 依赖
// conntrack 做 SNAT，表满会导致新连接被丢弃，因此这些参数直接影响服务可用性。
// 各字段用指针表示「未设置就不修改系统默认值」，避免零值覆盖内核现状。
type KubeProxyConntrackConfiguration struct {
	// maxPerCore is the maximum number of NAT connections to track
	// per CPU core (0 to leave the limit as-is and ignore min).
	//
	// 按 CPU 核数缩放表容量，是因为 conntrack 条目按 CPU 分片；设成 0 表示
	// 不接管这个上限，此时 Min 也会被忽略，避免两个来源的设置互相打架。
	MaxPerCore *int32 `json:"maxPerCore"`
	// min is the minimum value of connect-tracking records to allocate,
	// regardless of maxPerCore (set maxPerCore=0 to leave the limit as-is).
	//
	// 为小核数机器兜底：仅按 maxPerCore 缩放时，单核节点的表会过小。
	Min *int32 `json:"min"`
	// tcpEstablishedTimeout is how long an idle TCP connection will be kept open
	// (e.g. '2s').  Must be greater than 0 to set.
	//
	// 缩短这个超时可以让失效连接尽快从表中释放，但也会让长连接更容易被提前回收。
	TCPEstablishedTimeout *metav1.Duration `json:"tcpEstablishedTimeout"`
	// tcpCloseWaitTimeout is how long an idle conntrack entry
	// in CLOSE_WAIT state will remain in the conntrack
	// table. (e.g. '60s'). Must be greater than 0 to set.
	//
	// CLOSE_WAIT 条目会长期占用表项，设置较短的回收时间可防止表被半开连接耗尽。
	TCPCloseWaitTimeout *metav1.Duration `json:"tcpCloseWaitTimeout"`
	// tcpBeLiberal, if true, kube-proxy will configure conntrack
	// to run in liberal mode for TCP connections and packets with
	// out-of-window sequence numbers won't be marked INVALID.
	//
	// 宽松模式容忍乱序/重传报文，减少误判为 INVALID 而丢包的情况；
	// 代价是对异常报文的检查变松，所以默认不开启。
	TCPBeLiberal bool `json:"tcpBeLiberal"`
	// udpTimeout is how long an idle UDP conntrack entry in
	// UNREPLIED state will remain in the conntrack table
	// (e.g. '30s'). Must be greater than 0 to set.
	//
	// UDP 无连接状态，UNREPLIED 表示只看到一个方向；这个超时控制何时回收这类条目。
	UDPTimeout metav1.Duration `json:"udpTimeout"`
	// udpStreamTimeout is how long an idle UDP conntrack entry in
	// ASSURED state will remain in the conntrack table
	// (e.g. '300s'). Must be greater than 0 to set.
	//
	// ASSURED 表示双向都有流量；这类「伪连接」保留更久，以免切断仍在使用的流。
	UDPStreamTimeout metav1.Duration `json:"udpStreamTimeout"`
}

// KubeProxyWinkernelConfiguration contains Windows/HNS settings for
// the Kubernetes proxy server.
//
// KubeProxyWinkernelConfiguration 是 Windows kernelspace 模式的配置。
// Windows 上转发由 HNS（Host Network Service）完成，概念与 Linux 的 iptables/IPVS
// 完全不同，因此单独成组，且只在 Windows 节点上有意义。
type KubeProxyWinkernelConfiguration struct {
	// networkName is the name of the network kube-proxy will use
	// to create endpoints and policies
	//
	// HNS 的 endpoint 与 policy 都挂在某个网络对象上；不指定时 kube-proxy 会尝试
	// 自动选择一个，显式给定可避免多网络环境下选错。
	NetworkName string `json:"networkName"`
	// sourceVip is the IP address of the source VIP endpoint used for
	// NAT when loadbalancing
	//
	// 从 Pod 发往 Service VIP 的流量需要一个源 VIP 做 NAT，回包才能路由回宿主机。
	SourceVip string `json:"sourceVip"`
	// enableDSR tells kube-proxy whether HNS policies should be created
	// with DSR
	//
	// DSR（直接服务器返回）让后端 Pod 直接回包给客户端，绕过宿主机的负载均衡路径，
	// 可减少一跳，但需要网络配置配合，因此默认关闭。
	EnableDSR bool `json:"enableDSR"`
	// rootHnsEndpointName is the name of hnsendpoint that is attached to
	// l2bridge for root network namespace
	//
	// 用于把宿主机根网络命名空间接到 l2bridge 上，名字需与预先创建的 HNS endpoint 对应。
	RootHnsEndpointName string `json:"rootHnsEndpointName"`
	// forwardHealthCheckVip forwards service VIP for health check port on
	// Windows
	//
	// Windows 上健康检查请求发往 Service VIP，需要额外转发才能到达后端，
	// 否则探针会因连不上而误判 Pod 不健康。
	ForwardHealthCheckVip bool `json:"forwardHealthCheckVip"`
}

// DetectLocalConfiguration contains optional settings related to DetectLocalMode option
//
// DetectLocalConfiguration 为不同的本地流量判定方式提供参数。两种模式各用一个字段，
// 因为判定依据不同（网桥名 vs 接口名前缀），同时填写也没有意义，
// 由 DetectLocalMode 决定实际读取哪一个。
type DetectLocalConfiguration struct {
	// bridgeInterface is a bridge interface name. When DetectLocalMode is set to
	// LocalModeBridgeInterface, kube-proxy will consider traffic to be local if
	// it originates from this bridge.
	//
	// 用于把来自某个 CNI 网桥的流量识别为本节点流量，避免对发往本机 Pod 的流量做 SNAT。
	BridgeInterface string `json:"bridgeInterface"`
	// interfaceNamePrefix is an interface name prefix. When DetectLocalMode is set to
	// LocalModeInterfaceNamePrefix, kube-proxy will consider traffic to be local if
	// it originates from any interface whose name begins with this prefix.
	//
	// 用前缀匹配代替精确名称，是因为这类虚拟接口常带编号后缀，逐个配置不现实。
	InterfaceNamePrefix string `json:"interfaceNamePrefix"`
}

// 指令 +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object 让生成器为
// 该类型实现 runtime.Object 接口所需的 DeepCopyObject 方法。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeProxyConfiguration contains everything necessary to configure the
// Kubernetes proxy server.
//
// KubeProxyConfiguration 是用户可见的完整配置结构。之所以把所有选项放在一个外部版本类型里，
// 是因为 kube-proxy 的配置来源（命令行、文件、ConfigMap）最终都要归一成同一份数据；
// 内部表示与默认值处理由 cmd/kube-proxy 和 component-base 负责，这里只描述文件格式。
//
// 字段大体分组：元信息与通用客户端/日志设置、监听地址与进程调优、各代理模式参数、
// 本地流量判定和 conntrack。保留已废弃的 PortRange 等字段，是为了让旧配置文件仍能反序列化，
// 即使其中的值已不再起作用。
type KubeProxyConfiguration struct {
	metav1.TypeMeta `json:""`

	// featureGates is a map of feature names to bools that enable or disable alpha/experimental features.
	//
	// 把特性开关放在配置里而不是编译期常量，是为了让同一个二进制在不同集群中
	// 以不同成熟度运行。omitempty 避免在没有自定义开关时把它们写进序列化结果。
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// clientConnection specifies the kubeconfig file and client connection settings for the proxy
	// server to use when communicating with the apiserver.
	//
	// 复用 component-base 的连接配置结构，使所有组件的连接参数（kubeconfig 路径、
	// QPS/Burst、超时等）保持一致的字段和默认值语义。
	ClientConnection componentbaseconfigv1alpha1.ClientConnectionConfiguration `json:"clientConnection"`
	// logging specifies the options of logging.
	// Refer to [Logs Options](https://github.com/kubernetes/component-base/blob/master/logs/options.go)
	// for more information.
	//
	// 同样复用 component-base 的日志配置，避免每个组件各自定义日志级别和格式。
	Logging logsapi.LoggingConfiguration `json:"logging,omitempty"`

	// hostnameOverride, if non-empty, will be used as the name of the Node that
	// kube-proxy is running on. If unset, the node name is assumed to be the same as
	// the node's hostname.
	//
	// 容器化部署时主机名未必等于 Node 对象名，需要显式覆盖才能正确关联节点信息。
	HostnameOverride string `json:"hostnameOverride"`
	// bindAddress can be used to override kube-proxy's idea of what its node's
	// primary IP is. Note that the name is a historical artifact, and kube-proxy does
	// not actually bind any sockets to this IP.
	//
	// 名字有误导性：它只影响 kube-proxy 推断出的节点主 IP，并不真的监听该地址。
	// 保留旧字段名是为了兼容既有配置，重命名会破坏向后兼容。
	BindAddress string `json:"bindAddress"`
	// healthzBindAddress is the IP address and port for the health check server to
	// serve on, defaulting to "0.0.0.0:10256" (if bindAddress is unset or IPv4), or
	// "[::]:10256" (if bindAddress is IPv6).
	//
	// 健康检查地址跟随 bindAddress 的地址族，是为了让双栈节点上的探针地址保持一致。
	HealthzBindAddress string `json:"healthzBindAddress"`
	// metricsBindAddress is the IP address and port for the metrics server to serve
	// on, defaulting to "127.0.0.1:10249" (if bindAddress is unset or IPv4), or
	// "[::1]:10249" (if bindAddress is IPv6). (Set to "0.0.0.0:10249" / "[::]:10249"
	// to bind on all interfaces.)
	//
	// 默认只监听本机环回，是因为指标接口通常不应暴露给外部网络；
	// 需要 Prometheus 跨节点抓取时再显式改为通配地址。
	MetricsBindAddress string `json:"metricsBindAddress"`
	// bindAddressHardFail, if true, tells kube-proxy to treat failure to bind to a
	// port as fatal and exit
	//
	// 默认容忍端口占用（例如已有进程在同一地址提供指标），
	// 打开此开关则把绑定失败视为致命错误，便于严格部署环境发现问题。
	BindAddressHardFail bool `json:"bindAddressHardFail"`
	// enableProfiling enables profiling via web interface on /debug/pprof handler.
	// Profiling handlers will be handled by metrics server.
	//
	// pprof 接口挂在指标服务器上，是为了复用同一个监听地址，避免再多开一个端口。
	EnableProfiling bool `json:"enableProfiling"`
	// showHiddenMetricsForVersion is the version for which you want to show hidden metrics.
	//
	// 用于平滑下线指标：被隐藏的指标只对指定版本放行，给升级留出过渡期。
	ShowHiddenMetricsForVersion string `json:"showHiddenMetricsForVersion"`

	// mode specifies which proxy mode to use.
	//
	// 所有模式共用一个字段而非各自的配置段，是因为同一时间只能启用一种转发实现。
	Mode ProxyMode `json:"mode"`
	// iptables contains iptables-related configuration options.
	//
	// 四个模式配置段始终都存在，不用的那几段会被默认化并忽略；
	// 这样配置文件只需写当前模式的子结构，加载逻辑也不必按条件解析不同的 JSON 形状。
	IPTables KubeProxyIPTablesConfiguration `json:"iptables"`
	// ipvs contains ipvs-related configuration options.
	IPVS KubeProxyIPVSConfiguration `json:"ipvs"`
	// nftables contains nftables-related configuration options.
	NFTables KubeProxyNFTablesConfiguration `json:"nftables"`
	// winkernel contains winkernel-related configuration options.
	Winkernel KubeProxyWinkernelConfiguration `json:"winkernel"`

	// detectLocalMode determines mode to use for detecting local traffic, defaults to ClusterCIDR
	//
	// 默认用 ClusterCIDR 判定本地流量，因为它无需依赖 CNI 的具体网桥或接口命名。
	DetectLocalMode LocalMode `json:"detectLocalMode"`
	// detectLocal contains optional configuration settings related to DetectLocalMode.
	//
	// 具体判定参数单独放在一个结构里，是因为多数情况下用户根本不需要覆盖默认行为。
	DetectLocal DetectLocalConfiguration `json:"detectLocal"`
	// clusterCIDR is the CIDR range of the pods in the cluster. (For dual-stack
	// clusters, this can be a comma-separated dual-stack pair of CIDR ranges.). When
	// DetectLocalMode is set to ClusterCIDR, kube-proxy will consider
	// traffic to be local if its source IP is in this range. (Otherwise it is not
	// used.)
	//
	// 用字符串而非结构化类型，是为了同时容纳单栈和逗号分隔的双栈 CIDR 对，
	// 且与命令行参数及其它组件的配置表示保持一致。
	ClusterCIDR string `json:"clusterCIDR"`

	// nodePortAddresses is a list of CIDR ranges and/or keywords that expand to CIDR
	// ranges. NodePort services are only accessible on node IPs covered by the list.
	// Supported keywords: 'primary' (the Node object's primary IPv4 and/or IPv6
	// addresses), 'localhost' (127.0.0.0/8 and ::1/128), and 'all' (0.0.0.0/0 and ::/0).
	// Any combination of valid keywords and CIDRs may be included in the list.
	//
	// Serving NodePorts on loopback IPs is only supported in iptables mode (IPv4
	// only, see iptables.localhostNodePorts), and, only for TCP, in nftables mode when
	// the KubeProxyNFTablesLocalhostNodePorts feature gate is enabled and the list
	// explicitly includes loopback (e.g. 'localhost').
	//
	// If unset, this defaults to 'all' in iptables and ipvs mode, and to 'primary' in
	// nftables mode.
	//
	// 用关键字（primary/localhost/all）而不是枚举，是为了让同一份配置能表达
	// 任意 CIDR 与常用集合的组合，同时不必为每种组合新增字段。
	// 各模式的默认值不同，是因为 nftables 模式对回环地址上的 NodePort 支持有限。
	NodePortAddresses []string `json:"nodePortAddresses"`

	// oomScoreAdj is the oom-score-adj value for kube-proxy process. Values must be within
	// the range [-1000, 1000]
	//
	// 用指针区分「未设置」和「设为 0」；0 与系统默认 OOM 评分相同，
	// 不填写时则保持内核已经给进程算出的值。
	OOMScoreAdj *int32 `json:"oomScoreAdj"`
	// conntrack contains conntrack-related configuration options.
	//
	// 与模式配置同理，conntrack 参数对所有数据面模式通用，因此放在顶层。
	Conntrack KubeProxyConntrackConfiguration `json:"conntrack"`
	// configSyncPeriod is how often configuration from the apiserver is refreshed. Must be greater
	// than 0.
	//
	// 定期从 apiserver 拉取配置可以修正缓存偏差；设为 0 会导致持续请求，
	// 因此要求大于 0。
	ConfigSyncPeriod metav1.Duration `json:"configSyncPeriod"`

	// portRange was previously used to configure the userspace proxy, but is now unused.
	//
	// 保留这个字段纯粹是为了兼容旧配置文件——userspace 代理早已移除，
	// 删掉字段会让旧配置反序列化时报错。
	PortRange string `json:"portRange"`

	// windowsRunAsService, if true, enables Windows service control manager API integration.
	//
	// Windows 上以服务方式运行时需要与 SCM 交互；omitempty 让非 Windows 环境的
	// 配置输出保持简洁。
	WindowsRunAsService bool `json:"windowsRunAsService,omitempty"`
}

// ProxyMode represents modes used by the Kubernetes proxy server.
//
// Three modes of proxy are available on Linux platforms: `iptables`, `ipvs`, and
// `nftables`. One mode of proxy is available on Windows platforms: `kernelspace`.
//
// If the proxy mode is unspecified, a default proxy mode will be used (currently this
// is `iptables` on Linux and `kernelspace` on Windows). If the selected proxy mode cannot be
// used (due to lack of kernel support, missing userspace components, etc) then kube-proxy
// will exit with an error.
//
// 用字符串类型而不是自定义枚举常量，是为了让配置文件直接写模式名称，
// 同时保持跨版本兼容：新增模式无需修改这个类型本身。校验放在加载逻辑里，
// 无法使用选定模式时直接退出，而不是静默回退到别的模式——静默回退会掩盖配置错误。
type ProxyMode string

// LocalMode represents modes to detect local traffic from the node
//
// 本地流量判定同样是可配置字符串。判定结果决定是否跳过 SNAT：
// 误判会把本机 Pod 发出的流量错误地做地址转换，因此不同网络环境需要不同策略。
type LocalMode string
