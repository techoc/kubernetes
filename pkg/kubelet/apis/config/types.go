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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logsapi "k8s.io/component-base/logs/api/v1"
	tracingapi "k8s.io/component-base/tracing/api/v1"
)

// HairpinMode 表示 kubelet 应如何配置网络以处理 hairpin 数据包。
type HairpinMode string

// 处理 hairpin 数据包的不同方式枚举。
const (
	// 在对应容器运行时的容器 veth 上设置 hairpin 标志。
	HairpinVeth = "hairpin-veth"
	// 将容器网桥设为混杂模式，强制其接受 hairpin 数据包，
	// 即使网桥端口未设置该标志。
	PromiscuousBridge = "promiscuous-bridge"
	// 以上均不采用。若 kubelet 以此模式启动，且 kube-proxy 运行在 iptables 模式，
	// 则 hairpin 数据包会被容器网桥丢弃。
	HairpinNone = "none"
)

// ResourceChangeDetectionStrategy 表示内部管理器（secret、configmap）
// 发现对象变化的方式。
type ResourceChangeDetectionStrategy string

// kubelet 各管理器发现变化的策略枚举。
const (
	// GetChangeDetectionStrategy 表示 kubelet 直接从 apiserver 获取所需对象。
	GetChangeDetectionStrategy ResourceChangeDetectionStrategy = "Get"
	// TTLCacheChangeDetectionStrategy 表示 kubelet 对直接从 apiserver 获取的对象使用 TTL 缓存。
	TTLCacheChangeDetectionStrategy ResourceChangeDetectionStrategy = "Cache"
	// WatchChangeDetectionStrategy 表示 kubelet 使用 watch 观察其关注对象的变化。
	WatchChangeDetectionStrategy ResourceChangeDetectionStrategy = "Watch"
	// RestrictedTopologyManagerPolicy 表示 kubelet 只允许所请求资源在 NUMA 节点上
	// 对齐最优的 Pod。
	RestrictedTopologyManagerPolicy = "restricted"
	// BestEffortTopologyManagerPolicy 表示 kubelet 优先选择 CPU 与设备资源
	// 在 NUMA 上对齐的 Pod。
	BestEffortTopologyManagerPolicy = "best-effort"
	// NoneTopologyManagerPolicy 表示 kubelet 不感知 Pod 的 CPU 与设备资源的 NUMA 对齐情况。
	NoneTopologyManagerPolicy = "none"
	// SingleNumaNodeTopologyManagerPolicy 表示 kubelet 只允许 CPU 与设备资源
	// 对齐到单个 NUMA 节点的 Pod。
	SingleNumaNodeTopologyManagerPolicy = "single-numa-node"
	// ContainerTopologyManagerScope 表示拓扑策略按容器维度应用。
	ContainerTopologyManagerScope = "container"
	// PodTopologyManagerScope 表示拓扑策略按 Pod 维度应用。
	PodTopologyManagerScope = "pod"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeletConfiguration 保存 kubelet 的配置。
type KubeletConfiguration struct {
	metav1.TypeMeta

	// enableServer 启用 kubelet 的安全服务器。
	// 注意：kubelet 的非安全端口由 readOnlyPort 选项控制。
	EnableServer bool
	// staticPodPath 是存放待运行本地（静态）Pod 的目录路径，
	// 也可以指向单个静态 Pod 文件。
	StaticPodPath string
	// podLogsDir 是 kubelet 存放 Pod 日志文件的根目录。
	// 默认值："/var/log/pods/"
	// 注意：不建议使用临时目录作为日志目录，这可能在许多地方引发意外行为。
	PodLogsDir string
	// syncFrequency 是同步运行中容器与配置的最大周期。
	SyncFrequency metav1.Duration
	// fileCheckFrequency 是检查配置文件是否更新的间隔。
	FileCheckFrequency metav1.Duration
	// httpCheckFrequency 是通过 HTTP 检查新数据的间隔。
	HTTPCheckFrequency metav1.Duration
	// staticPodURL 是获取待运行静态 Pod 的 URL。
	StaticPodURL string
	// staticPodURLHeader 是访问 staticPodURL 时使用的 HTTP 请求头。
	StaticPodURLHeader map[string][]string `datapolicy:"token"`
	// address 是 kubelet 提供服务的 IP 地址（设为 0.0.0.0 表示所有网卡）。
	Address string
	// port 是 kubelet 提供服务的端口。
	Port int32
	// readOnlyPort 是 kubelet 无需认证/鉴权即可访问的只读端口（设为 0 表示禁用）。
	ReadOnlyPort int32
	// volumePluginDir 是搜索第三方卷插件的目录完整路径。
	VolumePluginDir string
	// providerID 若设置，表示外部提供方（如云厂商）用于识别特定节点的实例唯一 ID。
	ProviderID string
	// tlsCertFile 是包含 HTTPS x509 证书的文件（若存在 CA 证书，需拼接在服务器证书之后）。
	// 如果未提供 tlsCertFile 与 tlsPrivateKeyFile，将为公网地址生成自签名证书与密钥，
	// 并保存到 --cert-dir 指定的目录。
	TLSCertFile string
	// tlsPrivateKeyFile 是包含与 tlsCertFile 匹配的 x509 私钥的文件。
	TLSPrivateKeyFile string
	// TLSCipherSuites 是服务器允许的密码套件列表。
	// 注意 TLS 1.3 的密码套件不可配置。
	// 取值来自 tls 包常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	TLSCipherSuites []string
	// TLSCurvePreferences 是服务器允许的密钥交换机制集合，
	// 使用 Go crypto/tls 的 CurveID 数值表示。
	// 可用取值取决于所使用的 Go 版本。
	// 各 Go 版本支持的取值参见 https://pkg.go.dev/crypto/tls#CurveID。
	// 列表顺序会被忽略，Go 会按内部偏好顺序从中选择密钥交换机制。
	// 若为空，则使用 Go 的默认曲线。
	TLSCurvePreferences []int32
	// TLSMinVersion 是支持的最低 TLS 版本。
	// 取值来自 tls 包常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	TLSMinVersion string
	// rotateCertificates 启用客户端证书轮换。kubelet 会向 certificates.k8s.io API
	// 请求新证书，这需要审批者批准对应的证书签名请求。
	RotateCertificates bool
	// serverTLSBootstrap 启用服务器证书引导。kubelet 不再自签服务证书，
	// 而是向 certificates.k8s.io API 请求证书；这需要审批者批准证书签名请求，
	// 并且必须启用 RotateKubeletServerCertificate 特性门控。
	ServerTLSBootstrap bool
	// authentication 指定 kubelet 服务器请求的认证方式。
	Authentication KubeletAuthentication
	// authorization 指定 kubelet 服务器请求的鉴权方式。
	Authorization KubeletAuthorization
	// registryPullQPS 是每秒从镜像仓库拉取的次数上限；设为 0 表示不限制。
	RegistryPullQPS int32
	// registryBurst 是突发拉取的最大数量，允许暂时突增到该值，
	// 但仍不超过 registryPullQPS；仅在 registryPullQPS > 0 时生效。
	RegistryBurst int32
	// imagePullCredentialsVerificationPolicy 决定当 Pod 请求节点上已存在的镜像时
	// 如何校验拉取凭据：
	//   - NeverVerify
	//       - 节点上的任何人都可以使用节点上已有的任意镜像
	//   - NeverVerifyPreloadedImages
	//       - 由 kubelet 之外的其他组件拉取到节点的镜像，
	//         使用时无需重新校验拉取凭据
	//   - NeverVerifyAllowlistedImages
	//       - 与 "NeverVerifyPreloadedImages" 类似，但只有
	//         `preloadedImagesVerificationAllowlist` 中的节点镜像无需重新校验
	//   - AlwaysVerify
	//       - 所有镜像都需要重新校验凭据
	ImagePullCredentialsVerificationPolicy string
	// preloadedImagesVerificationAllowlist 指定在
	// "NeverVerifyAllowlistedImages" 策略下免于凭据重新校验的镜像列表。
	// 列表项支持以 "/*" 结尾的完整路径段通配符。
	// 仅应使用不带 tag 或 digest 的镜像 spec。
	PreloadedImagesVerificationAllowlist []string
	// eventRecordQPS 是每秒创建事件的上限；为 0 表示不限制。
	EventRecordQPS int32
	// eventBurst 是事件创建的突发上限，允许暂时突增到该值，
	// 但仍不超过 eventRecordQPS；仅在 eventRecordQPS > 0 时生效。
	EventBurst int32
	// enableDebuggingHandlers 启用用于日志收集以及在本地运行容器和命令的服务器端点。
	EnableDebuggingHandlers bool
	// enableContentionProfiling 在 enableDebuggingHandlers 为 true 时启用阻塞分析。
	EnableContentionProfiling bool
	// healthzPort 是 localhost 上 healthz 端点的端口（设为 0 表示禁用）。
	HealthzPort int32
	// healthzBindAddress 是 healthz 服务器监听的 IP 地址。
	HealthzBindAddress string
	// oomScoreAdj 是 kubelet 进程的 oom-score-adj 值，取值范围为 [-1000, 1000]。
	OOMScoreAdj int32
	// clusterDomain 是集群的 DNS 域。若设置，kubelet 会让所有容器在宿主机
	// 搜索域之外也搜索该域。
	ClusterDomain string
	// clusterDNS 是集群 DNS 服务器的 IP 地址列表。若设置，kubelet 会配置所有
	// 容器使用它进行 DNS 解析，而不是宿主机的 DNS 服务器。
	ClusterDNS []string
	// streamingConnectionIdleTimeout 是流式连接自动关闭前允许的最大空闲时间。
	// 已废弃：不再有任何效果。
	StreamingConnectionIdleTimeout metav1.Duration
	// nodeStatusUpdateFrequency 是 kubelet 计算节点状态并检查是否需要向 API server
	// 更新的频率。状态会在发生变化时上报，或在距上次上报达到
	// nodeStatusReportFrequency 时上报。
	NodeStatusUpdateFrequency metav1.Duration
	// nodeStatusReportFrequency 是节点状态未变化时 kubelet 向 API server 上报状态的频率。
	// 若检测到任何变化，kubelet 会忽略该频率并立即上报。
	NodeStatusReportFrequency metav1.Duration
	// nodeLeaseDurationSeconds 是 kubelet 设置在其对应 Lease 上的时长。
	NodeLeaseDurationSeconds int32
	// ImageMinimumGCAge 是未使用镜像被回收前的最小存活时间。
	// 该字段值必须大于 0。
	// 未设置或为 0 时默认 2m。
	ImageMinimumGCAge metav1.Duration
	// ImageMaximumGCAge 是镜像未被使用的最长存活时间，超过后会被回收。
	// 默认值为 "0s"，表示禁用该字段，即不会因长时间未使用而回收镜像。
	ImageMaximumGCAge metav1.Duration
	// imageGCHighThresholdPercent 是必定运行镜像回收的磁盘使用率百分比。
	// 该百分比按此字段值相对 100 计算。
	ImageGCHighThresholdPercent int32
	// imageGCLowThresholdPercent 是绝不运行镜像回收的磁盘使用率百分比，
	// 也是回收的目标下限。该百分比按此字段值相对 100 计算。
	ImageGCLowThresholdPercent int32
	// 计算并缓存所有 Pod 卷磁盘使用情况的频率。
	VolumeStatsAggPeriod metav1.Duration
	// KubeletCgroups 是用于隔离 kubelet 的 cgroups 绝对名称。
	KubeletCgroups string
	// SystemCgroups 是放置所有尚未在容器中的非内核进程的 cgroups 绝对名称。
	// 为空表示不放入容器；回滚该标志需要重启。
	SystemCgroups string
	// CgroupRoot 是 Pod 使用的根 cgroup。
	// 若启用 CgroupsPerQOS，则它是 QoS cgroup 层次的根。
	CgroupRoot string
	// 启用基于 QoS 的 cgroup 层次：为各 QoS 类别建立顶层 cgroup，
	// 所有 Burstable 与 BestEffort Pod 都运行在各自对应的顶层 QoS cgroup 下。
	CgroupsPerQOS bool
	// kubelet 在宿主机上操作 cgroup 使用的驱动（cgroupfs 或 systemd）。
	CgroupDriver string
	// SingleProcessOOMKill 为 true 时，在 cgroups v2 中不会为容器 cgroup 设置
	// `memory.oom.group`，容器内的进程会被逐个 OOM kill 而非整组终止。
	// 也就是说，为 true 时行为与 cgroups v1 保持一致。
	SingleProcessOOMKill *bool
	// CPUManagerPolicy 是使用的 CPU 管理器策略名称。
	CPUManagerPolicy string
	// CPUManagerPolicyOptions 是一组 key=value，用于设置额外选项以微调 CPU 管理器策略行为。
	CPUManagerPolicyOptions map[string]string
	// CPU 管理器的调谐周期。
	CPUManagerReconcilePeriod metav1.Duration
	// MemoryManagerPolicy 是使用的内存管理器策略名称；需要启用 MemoryManager 特性门控。
	MemoryManagerPolicy string
	// TopologyManagerPolicy 是使用的拓扑管理器策略名称。
	TopologyManagerPolicy string
	// TopologyManagerScope 表示拓扑管理器请求与提示提供者生成拓扑提示的范围。
	// 默认值："container"
	// +optional
	TopologyManagerScope string
	// TopologyManagerPolicyOptions 是一组 key=value，用于设置额外选项以微调
	// 拓扑管理器策略行为；需要同时启用 "TopologyManager" 与
	// "TopologyManagerPolicyOptions" 特性门控。
	TopologyManagerPolicyOptions map[string]string
	// QoS 资源预留百分比映射（目前仅支持内存）；需要启用 QOSReserved 特性门控。
	QOSReserved map[string]string
	// runtimeRequestTimeout 是所有运行时请求的超时时间，但拉取、日志、exec 与 attach
	// 等长时请求除外。
	RuntimeRequestTimeout metav1.Duration
	// hairpinMode 指定 kubelet 应如何配置容器网桥以处理 hairpin 数据包。
	// 设置该标志后，Service 中的端点访问自身 Service 时可以负载均衡回自己。取值：
	//   "promiscuous-bridge"：将容器网桥设为混杂模式。
	//   "hairpin-veth"：在容器 veth 接口上设置 hairpin 标志。
	//   "none"：不做任何处理。
	// 通常需要设置 --hairpin-mode=hairpin-veth 才能实现 hairpin NAT，
	// 因为 promiscuous-bridge 依赖名为 cbr0 的容器网桥存在。
	HairpinMode string
	// maxPods 是此 kubelet 上可运行的 Pod 数量上限。
	MaxPods int32
	// 用于分配 Pod IP 的 CIDR，仅在独立模式下使用；集群模式下由 master 下发。
	PodCIDR string
	// 每个 Pod 的最大进程数。若为 -1，kubelet 默认使用节点可分配的 pid 容量。
	PodPidsLimit int64
	// ResolverConfig 是作为容器 DNS 解析配置基础的 resolver 配置文件。
	ResolverConfig string
	// RunOnce 使 kubelet 只向 API server 查询一次 Pod，运行这些 Pod 以及静态 Pod 文件
	// 指定的 Pod 后退出。
	// 已废弃：不再有任何效果。
	RunOnce bool
	// cpuCFSQuota 为设置了 CPU limit 的容器启用 CPU CFS 配额限制。
	CPUCFSQuota bool
	// CPUCFSQuotaPeriod 设置 CPU CFS 配额周期，即 cpu.cfs_period_us，默认 100ms。
	// 默认值："100ms"。修改该值需要启用 CustomCPUCFSQuotaPeriod 特性门控。
	CPUCFSQuotaPeriod metav1.Duration
	// maxOpenFiles 是 kubelet 进程可打开的文件数。
	MaxOpenFiles int64
	// nodeStatusMaxImages 限制 Node.Status.Images 中上报的镜像数量。
	NodeStatusMaxImages int32
	// contentType 是发送给 apiserver 的请求内容类型。
	ContentType string
	// kubeAPIQPS 是与 kubernetes apiserver 通信时使用的 QPS。
	KubeAPIQPS int32
	// kubeAPIBurst 是与 kubernetes apiserver 通信时允许的突发量。
	KubeAPIBurst int32
	// serializeImagePulls 启用后要求 kubelet 逐个拉取镜像。
	SerializeImagePulls bool
	// MaxParallelImagePulls 设置并行拉取镜像的最大数量。
	MaxParallelImagePulls *int32
	// 硬驱逐阈值，将信号名映射到数量。例如：{"memory.available": "300Mi"}。
	// 部分默认信号仅适用于 Linux：nodefs.inodesFree。
	EvictionHard map[string]string
	// 软驱逐阈值，将信号名映射到数量。例如：{"memory.available": "300Mi"}。
	EvictionSoft map[string]string
	// 每个软驱逐信号的宽限期，将信号名映射到时长。例如：{"memory.available": "30s"}。
	EvictionSoftGracePeriod map[string]string
	// kubelet 退出驱逐压力状态前需要等待的时长。
	EvictionPressureTransitionPeriod metav1.Duration
	// 因触发软驱逐阈值而终止 Pod 时允许使用的最大宽限期（秒）。
	EvictionMaxPodGracePeriod int32
	// 最小回收量，将信号名映射到数量，表示资源处于压力下时 kubelet 执行 Pod 驱逐
	// 至少会回收的资源量。例如：{"imagefs.available": "2Gi"}。
	EvictionMinimumReclaim map[string]string
	// mergeDefaultEvictionSettings 表示应将 evictionHard、evictionSoft、
	// evictionSoftGracePeriod 与 evictionMinimumReclaim 的默认值与中配置值合并。
	// 配置中显式指定的信号优先，未指定的信号继承默认值。
	// 若为 false，只要配置中指定了任一信号，未指定的其他信号都会被置为 0。
	// 该字段仅对存在默认值的字段生效，目前只有 evictionHard 有默认值。
	MergeDefaultEvictionSettings bool
	// podsPerCore 是每核最大 Pod 数，不能超过 MaxPods；为 0 时忽略该字段。
	PodsPerCore int32
	// enableControllerAttachDetach 启用 Attach/Detach 控制器管理调度到本节点的卷
	// 挂载/卸载，并禁止 kubelet 执行任何 attach/detach 操作。
	EnableControllerAttachDetach bool
	// protectKernelDefaults 为 true 时，若内核标志不符合预期，kubelet 会报错；
	// 否则 kubelet 会尝试修改内核标志以符合预期。
	ProtectKernelDefaults bool
	// 为 true 时，kubelet 会在 iptables 中创建 KUBE-IPTABLES-HINT 链，
	// 向其他组件提示系统 iptables 的配置情况。
	MakeIPTablesUtilChains bool
	// iptablesMasqueradeBit 曾用于控制 KUBE-MARK-MASQ 链的创建。
	// 已废弃：不再有任何效果。
	IPTablesMasqueradeBit int32
	// iptablesDropBit 曾用于控制 KUBE-MARK-DROP 链的创建。
	// 已废弃：不再有任何效果。
	IPTablesDropBit int32
	// featureGates 将特性名映射到布尔值，用于启用或禁用 alpha/实验性特性。
	// 该字段会逐项覆盖 "k8s.io/kubernetes/pkg/features/kube_features.go"
	// 中的内置默认值。
	FeatureGates map[string]bool
	// 若节点启用了 swap，则让 kubelet 启动失败。
	FailSwapOn bool
	// memorySwap 配置容器工作负载可用的 swap 内存。
	// +featureGate=NodeSwap
	// +optional
	MemorySwap MemorySwapConfiguration
	// 容器日志文件轮转前的最大大小。例如："5Mi" 或 "256Ki"。
	ContainerLogMaxSize string
	// 一个容器可保留的日志文件数量上限。
	ContainerLogMaxFiles int32
	// 处理日志轮转请求时可并发的日志轮转 worker 最大数量。
	ContainerLogMaxWorkers int32
	// 检查容器日志是否需要轮转的间隔。
	ContainerLogMonitorInterval metav1.Duration
	// ConfigMapAndSecretChangeDetectionStrategy 是 configmap 与 secret 管理器的运行模式。
	ConfigMapAndSecretChangeDetectionStrategy ResourceChangeDetectionStrategy
	// 以逗号分隔的不安全 sysctl 或其模式（以 `*` 结尾）白名单。
	// 不安全组包括 `kernel.shm*`、`kernel.msg*`、`kernel.sem`、`fs.mqueue.*` 与 `net.*`。
	// 这些 sysctl 属于命名空间隔离，但默认不允许设置。
	// 例如："`kernel.msg*,net.ipv4.route.min_pmtu`"。
	// +optional
	AllowedUnsafeSysctls []string
	// DefaultPodSysctls 是将应用到所有 Pod 的默认 sysctl 集合，
	// 可被 pod spec.securityContext.sysctls 中的设置覆盖。
	// 支持命名空间隔离组：`kernel.shm*`、`kernel.msg*`、`kernel.sem`、`fs.mqueue.*`、
	// `net.*`、`kernel.domainname` 与 `user.*`。
	// 例如：{"net.ipv4.ip_forward": "1", "kernel.shmall": "1048576"}。
	// +optional
	DefaultPodSysctls map[string]string
	// kernelMemcgNotification 启用后，kubelet 会借助内核 memcg 通知判断是否跨越
	// 内存驱逐阈值，而不再轮询。
	KernelMemcgNotification bool

	/* 以下字段用于 Node Allocatable */

	// 一组 ResourceName=ResourceQuantity 对（如 cpu=200m,memory=150G,ephemeral-storage=1G,pid=100），
	// 描述为非 kubernetes 组件预留的资源。
	// 当前仅支持 cpu、memory 以及根文件系统的本地临时存储。
	// 详情参见 https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources。
	SystemReserved map[string]string
	// 一组 ResourceName=ResourceQuantity 对（如 cpu=200m,memory=150G,ephemeral-storage=1G,pid=100），
	// 描述为 kubernetes 系统组件预留的资源。
	// 当前仅支持 cpu、memory 以及根文件系统的本地临时存储。
	// 详情参见 https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources。
	KubeReserved map[string]string
	// 该标志帮助 kubelet 识别用于对操作系统守护进程强制执行 `SystemReserved` 资源预留的
	// 顶层 cgroup 绝对名称。更多信息参见 [Node Allocatable] 文档。
	SystemReservedCgroup string
	// 该标志帮助 kubelet 识别用于对 Kubernetes 节点系统守护进程强制执行 `KubeReserved`
	// 资源预留的顶层 cgroup 绝对名称。更多信息参见 [Node Allocatable] 文档。
	KubeReservedCgroup string
	// 该标志指定 kubelet 需要执行的各类 Node Allocatable 强制措施。
	// 可接受的值包括 `pods`、`system-reserved`、`system-reserved-compressible`、
	// `kube-reserved` 与 `kube-reserved-compressible`。更多信息参见 [Node Allocatable] 文档。
	EnforceNodeAllocatable []string
	// 该选项指定为宿主机级系统线程与 kubernetes 相关线程预留的 CPU 列表。
	// 相比 system-reserved 与 kube-reserved 的动态列表，这里提供的是“静态”列表。
	// 该选项会覆盖 system-reserved 与 kube-reserved 提供的 CPU。
	ReservedSystemCPUs string
	// 希望显示隐藏指标的目标前一版本。
	// 仅前一个次版本有意义，其他值不被允许。
	// 格式为 <major>.<minor>，例如 '1.16'。
	// 使用该格式是为了在下一版本隐藏更多指标时能提前察觉，
	// 而不是在其后一个版本被永久移除时才感到意外。
	ShowHiddenMetricsForVersion string
	// Logging 指定日志相关选项。
	// 更多信息参见 [Logs Options](https://github.com/kubernetes/component-base/blob/master/logs/options.go)。
	Logging logsapi.LoggingConfiguration
	// EnableSystemLogHandler 启用 /logs 处理器。
	EnableSystemLogHandler bool
	// EnableSystemLogQuery 在 /logs 端点上启用节点日志查询功能。
	// 该功能生效还需同时启用 EnableSystemLogHandler。
	// 启用该功能存在安全影响，建议仅在需要调试时开启，其余时间关闭。
	// +optional
	EnableSystemLogQuery bool
	// ShutdownGracePeriod 指定节点关停时延迟关机的总时长，也是 Pod 终止的总宽限期。
	// 默认 0 秒。
	// +featureGate=GracefulNodeShutdown
	// +optional
	ShutdownGracePeriod metav1.Duration
	// ShutdownGracePeriodCriticalPods 指定节点关停时用于终止关键 Pod 的时长，
	// 应小于 ShutdownGracePeriod。默认 0 秒。
	// 例如 ShutdownGracePeriod=30s、ShutdownGracePeriodCriticalPods=10s 时，
	// 关停期间前 20 秒用于优雅终止普通 Pod，最后 10 秒用于终止关键 Pod。
	// +featureGate=GracefulNodeShutdown
	// +optional
	ShutdownGracePeriodCriticalPods metav1.Duration
	// ShutdownGracePeriodByPodPriority 根据 Pod 关联的优先级类别值指定关停宽限期。
	// 收到关停请求时，kubelet 会按 Pod 优先级决定宽限期并开始终止节点上所有 Pod，
	// 然后等待所有 Pod 退出。
	// 数组中的每一项表示优先级落在该项与列表中下一个更高项之间的 Pod，
	// 在节点关停时的优雅关停时长。
	ShutdownGracePeriodByPodPriority []ShutdownGracePeriodByPodPriority
	// ReservedMemory 指定各 NUMA 节点的内存预留列表。
	// 该参数仅在内存管理器特性上下文中有效；内存管理器不会把预留内存分配给容器。
	// 例如，NUMA0 有 10Gi 内存且为其预留 1Gi，则内存管理器会认为只有 9Gi 可分配。
	// 可以为不同 NUMA 节点与内存类型指定不同数量。
	// 也可以完全省略该参数，但所有 NUMA 节点预留内存总量
	// 应等于 node allocatable 特性所指定的内存量。
	// 若至少一个 node allocatable 参数非零，则至少需要指定一个 NUMA 节点。
	// 同时应避免：
	// 1. 重复指定同一 NUMA 节点与内存类型但取值不同。
	// 2. 为任何内存类型指定零 limit。
	// 3. 指定机器上不存在的 NUMA 节点 ID。
	// 4. 使用除 memory 与 hugepages-<size> 之外的内存类型。
	ReservedMemory []MemoryReservation
	// EnableProfiling 启用 /debug/pprof 处理器。
	EnableProfilingHandler bool
	// EnableDebugFlagsHandler 启用 /debug/flags/v 处理器。
	EnableDebugFlagsHandler bool
	// SeccompDefault 启用后将 `RuntimeDefault` 作为所有工作负载的默认 seccomp 配置。
	SeccompDefault bool
	// MemoryThrottlingFactor 指定设置 cgroupv2 memory.high 以实施 MemoryQoS 时，
	// 与内存 limit 或节点可分配内存相乘的系数。
	// 减小该系数会为容器 cgroup 设定更低的高水位并加大回收压力，
	// 增大则回收压力更小。为 nil 时不设置 memory.high。
	// 详情参见 https://kep.k8s.io/2570。
	// 默认值：nil
	// +featureGate=MemoryQoS
	// +optional
	MemoryThrottlingFactor *float64
	// MemoryReservationPolicy 控制 kubelet 如何应用 cgroup v2 内存保护。
	// "None"（默认）：不为容器与 Pod 设置 memory.min，确保内核不会锁定硬内存。
	// "TieredReservation"：根据内存 request，为 Guaranteed Pod 设置 memory.min，
	// 为 Burstable Pod 设置 memory.low。Guaranteed 内存不会被内核回收；
	// Burstable 内存会被优先保留，但在极端压力下仍可被回收。
	// 详情参见 https://kep.k8s.io/2570。
	// 默认值：None
	// +featureGate=MemoryQoS
	// +optional
	MemoryReservationPolicy MemoryReservationPolicy
	// registerWithTaints 是 kubelet 注册自身时添加到 Node 对象上的污点数组。
	// 仅在 registerNode 为 true 且节点首次注册时生效。
	// +optional
	RegisterWithTaints []v1.Taint
	// registerNode 启用向 apiserver 自动注册节点。
	// +optional
	RegisterNode bool

	// Tracing 指定 OpenTelemetry 追踪客户端的版本化配置。
	// 详情参见 https://kep.k8s.io/2832。
	// +optional
	Tracing *tracingapi.TracingConfiguration

	// LocalStorageCapacityIsolation 启用本地临时存储隔离特性，默认为 true。
	// 该特性允许用户为容器的临时存储设置 request/limit，并像 CPU、内存一样管理；
	// 也允许为 emptyDir 卷设置 sizeLimit，卷使用超过限制时会触发 Pod 驱逐。
	// 该特性依赖正确检测根文件系统磁盘使用量的能力。对于 kind rootless 等
	// 无法支持该能力的系统，应禁用 LocalStorageCapacityIsolation。
	// 禁用后，不应为容器临时存储设置 request/limit，也不应为 emptyDir 设置 sizeLimit。
	// +optional
	LocalStorageCapacityIsolation bool

	// ContainerRuntimeEndpoint 是容器运行时的端点。
	// Linux 支持 Unix 域套接字，Windows 支持命名管道与 TCP 端点。
	// 示例：'unix:///path/to/runtime.sock'、'npipe:////./pipe/runtime'。
	ContainerRuntimeEndpoint string

	// ImageServiceEndpoint 是容器镜像服务的端点；未指定时默认使用 ContainerRuntimeEndpoint。
	// +optional
	ImageServiceEndpoint string

	// FailCgroupV1 阻止 kubelet 在使用 cgroup v1 的宿主机上启动。
	// 默认为 'false'，即除非显式启用该选项，kubelet 允许在 cgroup v1 宿主机上启动。
	// +optional
	FailCgroupV1 bool

	// CrashLoopBackOff 包含用于调整节点级容器重启行为参数的配置。
	// +featureGate=KubeletCrashLoopBackoffMax
	// +optional
	CrashLoopBackOff CrashLoopBackOffConfig

	// UserNamespaces 包含用户命名空间相关配置。
	// +featureGate=UserNamespacesSupport
	// +optional
	UserNamespaces *UserNamespaces
}

// KubeletAuthorizationMode 表示 kubelet 的鉴权模式。
type KubeletAuthorizationMode string

const (
	// KubeletAuthorizationModeAlwaysAllow 允许所有已认证的请求。
	KubeletAuthorizationModeAlwaysAllow KubeletAuthorizationMode = "AlwaysAllow"
	// KubeletAuthorizationModeWebhook 使用 SubjectAccessReview API 判定鉴权。
	KubeletAuthorizationModeWebhook KubeletAuthorizationMode = "Webhook"
)

// KubeletAuthorization 保存 kubelet 鉴权相关配置。
type KubeletAuthorization struct {
	// mode 是应用于 kubelet 服务器请求的鉴权模式。
	// 合法值为 AlwaysAllow 与 Webhook。
	// Webhook 模式使用 SubjectAccessReview API 判定鉴权。
	Mode KubeletAuthorizationMode

	// webhook 包含 Webhook 鉴权相关设置。
	Webhook KubeletWebhookAuthorization
}

// KubeletWebhookAuthorization 保存 kubelet 中 Webhook 鉴权相关配置。
type KubeletWebhookAuthorization struct {
	// cacheAuthorizedTTL 是缓存 webhook 鉴权器 'authorized' 响应的时长。
	CacheAuthorizedTTL metav1.Duration
	// cacheUnauthorizedTTL 是缓存 webhook 鉴权器 'unauthorized' 响应的时长。
	CacheUnauthorizedTTL metav1.Duration
}

// KubeletAuthentication 保存 kubelet 的认证设置。
type KubeletAuthentication struct {
	// x509 包含 x509 客户端证书认证相关设置。
	X509 KubeletX509Authentication
	// webhook 包含 Webhook 的 bearer token 认证设置。
	Webhook KubeletWebhookAuthentication
	// anonymous 包含匿名认证相关设置。
	Anonymous KubeletAnonymousAuthentication
}

// KubeletX509Authentication 包含 x509 客户端证书认证相关设置。
type KubeletX509Authentication struct {
	// clientCAFile 是 PEM 编码证书包的路径。若设置，任何携带由包内某一 CA 签发的
	// 客户端证书的请求都会通过认证，用户名取自证书的 CommonName，
	// 组取自证书的 Organization。
	ClientCAFile string
}

// KubeletWebhookAuthentication 包含 webhook 认证相关设置。
type KubeletWebhookAuthentication struct {
	// enabled 允许使用由 tokenreviews.authentication.k8s.io API 支持的 bearer token 认证。
	Enabled bool
	// cacheTTL 启用认证结果缓存。
	CacheTTL metav1.Duration
}

// KubeletAnonymousAuthentication 控制是否允许匿名请求访问 kubelet 服务器。
type KubeletAnonymousAuthentication struct {
	// enabled 允许匿名请求访问 kubelet 服务器。
	// 未被其他认证方式拒绝的请求会按匿名请求处理。
	// 匿名请求的用户名为 system:anonymous，组名为 system:unauthenticated。
	Enabled bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SerializedNodeConfigSource 用于序列化 NodeConfigSource。
// 该类型由 kubelet 内部使用，用于跟踪 checkpoint 的动态配置。
// 它属于 kubeletconfig API 组，因为其被归类为 kubelet 的版本化输入。
type SerializedNodeConfigSource struct {
	metav1.TypeMeta
	// Source 是正在被序列化的来源。
	// +optional
	Source v1.NodeConfigSource
}

// ServiceAccountTokenCacheType 是使用 ServiceAccount Token 时，缓存插件返回凭据所用的缓存键类型。
type ServiceAccountTokenCacheType string

const (
	// TokenServiceAccountTokenCacheType 表示 kubelet 按 Token 维度缓存返回的凭据。
	// 若返回凭据的生命周期受输入 ServiceAccount Token 生命周期限制，应使用该值。
	// 例如，直接把输入 ServiceAccount Token 作为拉取凭据返回时必须使用该值。
	TokenServiceAccountTokenCacheType ServiceAccountTokenCacheType = "Token"
	// ServiceAccountServiceAccountTokenCacheType 表示 kubelet 按 ServiceAccount 维度缓存返回的凭据。
	// 若插件的凭据获取逻辑只依赖 ServiceAccount 而不依赖 Pod 特定声明，应使用该值。
	// 返回凭据对使用同一 ServiceAccount 的所有 Pod 都有效时使用该值。
	ServiceAccountServiceAccountTokenCacheType ServiceAccountTokenCacheType = "ServiceAccount"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderConfig 包含各 exec 凭据插件的信息。
// kubelet 从磁盘读取该配置，并按 CredentialProvider 类型启用各插件。
type CredentialProviderConfig struct {
	metav1.TypeMeta

	// providers 是 kubelet 将启用的凭据插件列表。
	// 多个插件可能匹配同一镜像，此时所有插件的凭据都会返回给 kubelet；
	// 若为同一镜像调用多个插件，结果会合并。若插件返回的认证键重叠，
	// 则优先尝试列表中靠前插件的值。
	Providers []CredentialProvider
}

// CredentialProvider 表示由 kubelet 调用的 exec 插件。
// 仅当拉取的镜像匹配插件处理的镜像时（见 matchImages）才会调用该插件。
type CredentialProvider struct {
	// name 是凭据插件的必填名称，必须与 kubelet 看到的插件可执行文件名称一致。
	// 可执行文件必须位于 kubelet 的 bin 目录（由 --credential-provider-bin-dir 指定），
	// 且在所有插件中唯一。
	Name string

	// matchImages 是用于匹配镜像的必填字符串列表，用于判断是否调用该插件。
	// 若其中任一项匹配 kubelet 请求的镜像，插件会被调用并获得提供凭据的机会。
	// 镜像应包含 registry 域与 URL 路径。
	//
	// matchImages 中的每一项是一个模式，可选地包含端口与路径。
	// 域中可以使用通配符，端口与路径中不行。通配符支持子域形式如 `*.k8s.io`
	// 或 `k8s.*.io`，以及顶级域形式如 `k8s.*`。也支持匹配部分子域，如 `app*.k8s.io`。
	// 每个通配符只能匹配一个子域段，因此 `*.io` 不会匹配 *.k8s.io。
	//
	// 当以下条件全部满足时，镜像与 matchImage 视为匹配：
	// - 两者的域段数量相同且各段匹配。
	// - imageMatch 的 URL 路径必须是目标镜像 URL 路径的前缀。
	// - 若 imageMatch 包含端口，则镜像中的端口也必须一致。
	//
	// matchImages 示例：
	//   - `123456789.dkr.ecr.us-east-1.amazonaws.com`
	//   - `*.azurecr.io`
	//   - `gcr.io`
	//   - `*.*.registry.io`
	//   - `registry.io:8080/path`
	MatchImages []string

	// defaultCacheDuration 是插件响应未提供缓存时长时，在内存中缓存凭据的默认时长。
	// 该字段必填。
	DefaultCacheDuration *metav1.Duration

	// exec CredentialProviderRequest 的必需输入版本。返回的 CredentialProviderResponse
	// 必须使用与输入相同的编码版本。当前支持的取值：
	// - credentialprovider.kubelet.k8s.io/v1alpha1
	// - credentialprovider.kubelet.k8s.io/v1beta1
	// - credentialprovider.kubelet.k8s.io/v1
	APIVersion string

	// 执行命令时传入的参数。
	// +optional
	Args []string

	// Env 定义暴露给进程的额外环境变量，会与宿主机环境以及 client-go
	// 向插件传递参数所用的变量取并集。
	// +optional
	Env []ExecEnvVar

	// tokenAttributes 指定将传递给插件的 ServiceAccount Token 配置。
	// 凭据插件通过设置该字段来选择使用 ServiceAccount Token 进行镜像拉取。
	// 设置后，kubelet 会为拉取镜像的 Pod 生成绑定的 ServiceAccount Token，
	// 并连同插件所需的其他属性通过 CredentialProviderRequest 传给插件。
	//
	// ServiceAccount 元数据与 Token 属性会作为 kubelet 缓存凭据的维度。
	// 缓存键由 ServiceAccount 元数据（namespace、name、UID，以及
	// serviceAccountTokenAttribute.requiredServiceAccountAnnotationKeys 与
	// optionalServiceAccountAnnotationKeys 中定义的注解 key+value）组合生成。
	// ServiceAccount Token 中的 Pod 元数据（namespace、name、UID）不作为 kubelet
	// 缓存凭据的维度。这意味着使用同一 ServiceAccount 的工作负载可能共用镜像拉取凭据。
	// 不希望如此、或以透传模式运行的插件（即原样返回 ServiceAccount Token），
	// 可将 credentialProviderResponse.cacheDuration 设为 0 以禁用 kubelet 内的凭据缓存，
	// 此时每次镜像拉取都会调用插件。这样会增加每次拉取的 Token 生成开销，
	// 但能确保凭据不在 Pod 之间共享（即使它们使用同一 ServiceAccount）。
	// +optional
	TokenAttributes *ServiceAccountTokenAttributes
}

// ServiceAccountTokenAttributes 指定将传递给插件的 ServiceAccount Token 配置。
type ServiceAccountTokenAttributes struct {
	// serviceAccountTokenAudience 是投影 ServiceAccount Token 的目标受众。
	// +required
	ServiceAccountTokenAudience string

	// cacheType 表示使用 ServiceAccount Token 时，缓存插件返回凭据所用的缓存键类型。
	// 最保守的选项是 "Token"，即 kubelet 按 Token 维度缓存返回凭据；
	// 若返回凭据的生命周期受 ServiceAccount Token 生命周期限制，应使用该值。
	// 若插件的凭据获取逻辑只依赖 ServiceAccount 而不依赖 Pod 特定声明，
	// 可设为 "ServiceAccount"，此时 kubelet 按 ServiceAccount 维度缓存返回凭据。
	// 返回凭据对使用同一 ServiceAccount 的所有 Pod 都有效时使用该值。
	// +required
	CacheType ServiceAccountTokenCacheType

	// requireServiceAccount 表示插件是否要求 Pod 具有 ServiceAccount。
	// 设为 true 时，仅当 Pod 有 ServiceAccount 才调用插件。
	// 设为 false 时，即使 Pod 没有 ServiceAccount 也会调用插件，
	// 且这种情况下 CredentialProviderRequest 中不包含 Token。
	// 这适用于为无 ServiceAccount 的 Pod（如静态 Pod）拉取镜像的插件。
	// +required
	RequireServiceAccount *bool

	// requiredServiceAccountAnnotationKeys 是插件关注且要求 ServiceAccount 必须存在的注解键列表。
	// 列表中定义的键会从对应 ServiceAccount 中提取，并作为 CredentialProviderRequest 的一部分传给插件。
	// 若其中任一键在 ServiceAccount 中不存在，kubelet 不会调用插件并返回错误。
	// 该字段可选，可以为空。插件可用它提取获取凭据所需的额外信息，
	// 或允许工作负载选择使用 ServiceAccount Token 进行镜像拉取。
	// 非空时 requireServiceAccount 必须为 true。
	// 列表中的键必须唯一，且不能与 optionalServiceAccountAnnotationKeys 重叠。
	// +optional
	RequiredServiceAccountAnnotationKeys []string

	// optionalServiceAccountAnnotationKeys 是插件关注但不强制存在的注解键列表。
	// 列表中定义的键会从对应 ServiceAccount 中提取并传给插件；
	// 插件负责校验这些注解是否存在及其取值。
	// 该字段可选，可以为空。插件可用它提取获取凭据所需的额外信息。
	// 列表中的键必须唯一。
	// +optional
	OptionalServiceAccountAnnotationKeys []string
}

// ExecEnvVar 用于在执行基于 exec 的凭据插件时设置环境变量。
type ExecEnvVar struct {
	Name  string
	Value string
}

// MemoryReservation 指定各 NUMA 节点上不同类型的内存预留。
type MemoryReservation struct {
	NumaNode int32
	Limits   v1.ResourceList
}

// ShutdownGracePeriodByPodPriority 根据 Pod 关联的优先级类别值指定关停宽限期。
type ShutdownGracePeriodByPodPriority struct {
	// priority 是与此关停宽限期关联的优先级值。
	Priority int32
	// shutdownGracePeriodSeconds 是以秒为单位的关停宽限期。
	ShutdownGracePeriodSeconds int64
}

// MemorySwapConfiguration 配置容器工作负载的 swap 使用方式。
type MemorySwapConfiguration struct {
	// swapBehavior 配置容器工作负载可用的 swap 内存，取值可以是：
	// ""、"NoSwap"：工作负载不能使用 swap，这是默认选项。
	// "LimitedSwap"：限制工作负载的 swap 使用量，限制与容器内存 request 成比例。
	// +featureGate=NodeSwap
	// +optional
	SwapBehavior string
}

// CrashLoopBackOffConfig 用于配置该 kubelet 的容器重启行为。
type CrashLoopBackOffConfig struct {
	// MaxContainerRestartPeriod 是容器重启退避延迟可累积到的最大值，
	// 最小 1 秒，最大 300 秒。
	// +featureGate=KubeletCrashLoopBackOffMax
	// +optional
	MaxContainerRestartPeriod *metav1.Duration
}

// ImagePullCredentialsVerificationPolicy 是 Pod 请求系统中已有镜像时执行的策略枚举。
type ImagePullCredentialsVerificationPolicy string

const (
	// NeverVerify 对节点上已存在的镜像从不要求凭据校验。
	NeverVerify ImagePullCredentialsVerificationPolicy = "NeverVerify"
	// NeverVerifyPreloadedImages 对 kubelet 进程之外拉取的镜像不要求凭据校验。
	NeverVerifyPreloadedImages ImagePullCredentialsVerificationPolicy = "NeverVerifyPreloadedImages"
	// NeverVerifyAllowlistedImages 对 kubelet 进程之外拉取的白名单镜像不要求凭据校验。
	NeverVerifyAllowlistedImages ImagePullCredentialsVerificationPolicy = "NeverVerifyAllowlistedImages"
	// AlwaysVerify 对节点上任何镜像的访问都要求凭据校验，不论其如何被拉取。
	AlwaysVerify ImagePullCredentialsVerificationPolicy = "AlwaysVerify"
)

// MemoryReservationPolicy 定义 kubelet 如何应用 cgroup v2 内存保护。
type MemoryReservationPolicy string

const (
	// NoneMemoryReservationPolicy 禁用容器与 Pod 的 memory.min 保护。
	// 这是默认值，通过避免内存被“锁定”来维持节点稳定性。
	NoneMemoryReservationPolicy MemoryReservationPolicy = "None"
	// TieredReservationMemoryReservationPolicy 启用分层内存保护：
	// Guaranteed Pod 使用 memory.min，Burstable Pod 使用 memory.low。
	TieredReservationMemoryReservationPolicy MemoryReservationPolicy = "TieredReservation"
)

// ImagePullIntent 记录 kubelet 尝试拉取镜像的意图。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ImagePullIntent struct {
	metav1.TypeMeta

	// Image 来自容器 `image` 字段的镜像 spec。
	// 文件名是该值的 SHA-256 哈希，以避免 ':'、'/' 等不适合作为文件名的字符。
	Image string
}

// ImagePullRecord 记录 kubelet 已拉取的镜像。
//
// 若 `kubernetesSecrets` 字段没有记录，且 `nodeWideCredentials` 与 `anonymous`
// 都为 `false`，则下次请求该记录对应的镜像时必须重新校验凭据。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ImagePulledRecord struct {
	metav1.TypeMeta

	// LastUpdatedTime 是此记录最后一次更新的时间。
	LastUpdatedTime metav1.Time

	// ImageRef 是从 CRI 收到的、此文件所代表镜像的引用。
	// 文件名是该值的 SHA-256 哈希，以避免 ':'、'/' 等不适合作为文件名的字符。
	ImageRef string

	// CredentialMapping 将 `image` 映射到其此前拉取时使用的凭据集合。
	// 此处的 `image` 是 Pod 容器 `image` 字段去掉 tag/digest 后的内容。
	//
	// 示例：
	//   容器请求 `hello-world:latest@sha256:91fb4b041da273d5a3273b6d587d62d518300a6ad268b28628f74997b93171b2` 镜像：
	//     "credentialMapping": {
	//       "hello-world": { "nodePodsAccessible": true }
	//     }
	CredentialMapping map[string]ImagePullCredentials
}

// ImagePullCredentials 描述可用于拉取镜像的凭据。
type ImagePullCredentials struct {
	// KubernetesSecrets 是曾用于拉取该镜像的所有 kubernetes secret 的坐标索引。
	// +optional
	KubernetesSecrets []ImagePullSecret

	// KubernetesServiceAccounts 是曾用于拉取该镜像的所有 kubernetes ServiceAccount 的坐标索引。
	// +optional
	KubernetesServiceAccounts []ImagePullServiceAccount

	// NodePodsAccessible 表示拉取凭据对节点上所有 Pod 可访问，或拉取无需凭据。
	//
	// 为 true 时，与 `kubernetesSecrets` 和 `kubernetesServiceAccounts` 字段互斥。
	// +optional
	NodePodsAccessible bool
}

// ImagePullSecret 表示 Kubernetes secret 对象坐标，以及该对象所含拉取凭据的哈希。
type ImagePullSecret struct {
	UID       string
	Namespace string
	Name      string

	// CredentialHash 是对 UID/Namespace/Name 指定 secret 的镜像拉取凭据内容做哈希得到的 SHA-256。
	CredentialHash string
}

// ImagePullServiceAccount 表示 Kubernetes ServiceAccount 对象坐标，
// kubelet 曾向其对应的 ServiceAccount 发送 Token 给凭据插件以获取镜像拉取凭据。
type ImagePullServiceAccount struct {
	UID       string
	Namespace string
	Name      string
}

// UserNamespaces 包含用户命名空间相关配置。
type UserNamespaces struct {
	// IDsPerPod 是 UID 与 GID 的映射长度。
	// 长度必须是 65536 的倍数，且小于 1<<32。
	// 在 Windows 等非 Linux 平台上只允许为 null 或缺省。
	//
	// 修改该值可能需要重建节点上的所有容器。
	//
	// 默认值：65536
	// +featureGate=UserNamespacesSupport
	// +optional
	IDsPerPod *int64
}
