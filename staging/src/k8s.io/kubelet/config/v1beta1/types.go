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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logsapi "k8s.io/component-base/logs/api/v1"
	tracingapi "k8s.io/component-base/tracing/api/v1"
)

// HairpinMode 定义 kubelet 如何处理发夹（hairpin）报文的网络配置。
type HairpinMode string

// 处理发夹报文的各种方式对应的枚举值。
const (
	// 在相应容器运行时的容器 veth 上设置 hairpin 标志。
	HairpinVeth = "hairpin-veth"
	// 把容器网桥设为混杂模式，强制其接受发夹报文，
	// 即使网桥端口上未设置该标志。
	PromiscuousBridge = "promiscuous-bridge"
	// 以上都不用：此模式下若 kube-proxy 使用 iptables，发夹包会被容器网桥丢弃。
	HairpinNone = "none"
)

// ResourceChangeDetectionStrategy 定义内部管理器（secret、configmap）
// 发现对象变化的方式。
type ResourceChangeDetectionStrategy string

// kubelet 各管理器的变更发现策略枚举。
const (
	// GetChangeDetectionStrategy：kubelet 直接从 apiserver 拉取所需对象。
	// 直接从 apiserver 获取所需对象。
	GetChangeDetectionStrategy ResourceChangeDetectionStrategy = "Get"
	// TTLCacheChangeDetectionStrategy：kubelet 对直接从 apiserver 拉取的对象使用 ttl 缓存。
	// 对直接从 apiserver 获取的对象使用 TTL 缓存。
	TTLCacheChangeDetectionStrategy ResourceChangeDetectionStrategy = "Cache"
	// WatchChangeDetectionStrategy：kubelet 通过 watch 监听关注对象的变化。
	// 通过 watch 监听关注对象的变化。
	WatchChangeDetectionStrategy ResourceChangeDetectionStrategy = "Watch"
	// RestrictedTopologyManagerPolicy：kubelet 只接收所请求资源在 NUMA 节点上最优对齐的 Pod。
	// NUMA 对齐最优的 Pod。
	RestrictedTopologyManagerPolicy = "restricted"
	// BestEffortTopologyManagerPolicy：kubelet 倾向接收 CPU 与设备资源 NUMA 对齐的 Pod。
	// CPU 与设备资源 NUMA 对齐的 Pod。
	BestEffortTopologyManagerPolicy = "best-effort"
	// NoneTopologyManagerPolicy：kubelet 不感知 Pod CPU 与设备资源的 NUMA 对齐情况。
	// Pod 的 CPU 与设备资源 NUMA 对齐情况。
	NoneTopologyManagerPolicy = "none"
	// SingleNumaNodeTopologyManagerPolicy：kubelet 只接收 CPU 与设备资源位于单个 NUMA 节点上的 Pod。
	// CPU 与设备资源位于单个 NUMA 节点上的 Pod。
	SingleNumaNodeTopologyManagerPolicy = "single-numa-node"
	// ContainerTopologyManagerScope 表示
	// 拓扑策略按容器粒度应用。
	ContainerTopologyManagerScope = "container"
	// PodTopologyManagerScope 表示
	// 拓扑策略按 Pod 粒度应用。
	PodTopologyManagerScope = "pod"
	// NoneMemoryManagerPolicy 是内存管理器的 none 策略：不固定 guaranteed Pod 容器的内存。
	NoneMemoryManagerPolicy = "None"
	// StaticMemoryManagerPolicy 是静态策略：尽量把 guaranteed Pod 容器的内存固定在
	// 尽可能小的 NUMA 节点子集上。
	StaticMemoryManagerPolicy = "Static"
)

// ImagePullCredentialsVerificationPolicy 定义 Pod 请求节点上已存在的镜像时
// 执行的凭据校验策略枚举。
type ImagePullCredentialsVerificationPolicy string

const (
	// NeverVerify：对节点上已存在的镜像从不要求凭据校验。
	// （节点上已存在的镜像）。
	NeverVerify ImagePullCredentialsVerificationPolicy = "NeverVerify"
	// NeverVerifyPreloadedImages：对 kubelet 之外拉取到节点上的镜像不要求凭据校验。
	// （kubelet 之外拉取的镜像）。
	NeverVerifyPreloadedImages ImagePullCredentialsVerificationPolicy = "NeverVerifyPreloadedImages"
	// NeverVerifyAllowlistedImages：对在 kubelet 之外拉取且在允许列表中的镜像不要求凭据校验。
	// （由 kubelet 之外方式拉取的镜像）。
	NeverVerifyAllowlistedImages ImagePullCredentialsVerificationPolicy = "NeverVerifyAllowlistedImages"
	// AlwaysVerify：对节点上任何镜像都要求凭据校验，无论其拉取方式。
	// 无论其拉取方式。
	AlwaysVerify ImagePullCredentialsVerificationPolicy = "AlwaysVerify"
)

// MemoryReservationPolicy 定义 kubelet 如何应用 cgroup v2 内存保护。
type MemoryReservationPolicy string

const (
	// NoneMemoryReservationPolicy 禁用容器与 Pod 的 memory.min 保护。
	// 这是默认值，避免内存被"锁定"以保持节点稳定。
	NoneMemoryReservationPolicy MemoryReservationPolicy = "None"
	// TieredReservationMemoryReservationPolicy 启用分级内存保护：
	// Guaranteed Pod 用 memory.min，Burstable Pod 用 memory.low。
	TieredReservationMemoryReservationPolicy MemoryReservationPolicy = "TieredReservation"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeletConfiguration 包含 kubelet 的配置。
type KubeletConfiguration struct {
	metav1.TypeMeta `json:""`

	// 安全服务开关；只读端口的安全由 readOnlyPort 单独控制，两者互不影响。
	// enableServer 启用 kubelet 的安全服务。
	// 注意：kubelet 的非安全端口由 readOnlyPort 选项控制。
	// 默认值：true
	EnableServer *bool `json:"enableServer,omitempty"`
	// 静态 Pod 来源可以是目录也可以是单个文件；kubelet 直接监控本地路径，不经 apiserver。
	// staticPodPath 是存放本地静态 Pod 的目录路径，或单个静态 Pod 文件的路径。
	// 或单个静态 Pod 文件的路径。
	// 默认值：""
	// +optional
	StaticPodPath string `json:"staticPodPath,omitempty"`
	// 日志根目录不宜放在临时目录，否则清理策略与路径重建会引发不可预期行为。
	// podLogsDir 是 kubelet 放置 Pod 日志文件的自定义根目录路径。
	// 默认值："/var/log/pods/"
	// 注意：不建议把临时目录用作日志目录，可能在多处引发意外行为。
	// unexpected behavior in many places.
	// +optional
	PodLogsDir string `json:"podLogsDir,omitempty"`
	// 同步周期是Pod状态收敛速度上限，过于频繁会放大 apiserver 与运行时压力。
	// syncFrequency 是同步运行中容器与配置的最大周期。
	// containers and config.
	// 默认值："1m"
	// +optional
	SyncFrequency metav1.Duration `json:"syncFrequency,omitempty"`
	// 文件检查周期决定静态 Pod 与清单文件的变更感知延迟。
	// fileCheckFrequency 是检查配置文件是否有新数据的间隔。
	// new data.
	// 默认值："20s"
	// +optional
	FileCheckFrequency metav1.Duration `json:"fileCheckFrequency,omitempty"`
	// HTTP 检查周期决定从 URL 获取静态 Pod 清单的更新频率。
	// httpCheckFrequency 是检查 http 是否有新数据的间隔。
	// Default: "20s"
	// +optional
	HTTPCheckFrequency metav1.Duration `json:"httpCheckFrequency,omitempty"`
	// 通过 URL 获取静态 Pod 清单时，kubelet 会按 HTTPCheckFrequency 轮询。
	// staticPodURL 是访问要运行的静态 Pod 的 URL。
	// 默认值：""
	// +optional
	StaticPodURL string `json:"staticPodURL,omitempty"`
	// 为拉取静态 Pod 清单的 HTTP 请求附加自定义头，可用于携带认证信息。
	// staticPodURLHeader 是访问 podURL 时使用的 HTTP 头映射（切片）。
	// 默认值：nil
	// +optional
	StaticPodURLHeader map[string][]string `json:"staticPodURLHeader,omitempty"`
	// 监听地址设为 0.0.0.0 会暴露所有网卡，需配合认证授权使用。
	// address 是 kubelet 提供服务的 IP 地址（设为 0.0.0.0 表示监听所有网卡）。
	// for all interfaces).
	// 默认值："0.0.0.0"
	// +optional
	Address string `json:"address,omitempty"`
	// 这是 kubelet 安全服务端口，与只读端口分开。
	// port 是 kubelet 提供服务的端口。
	// 端口号必须在 1 到 65535（含）之间。
	// 默认值：10250
	// +optional
	Port int32 `json:"port,omitempty"`
	// 只读端口无认证授权，生产环境应保持禁用；设为 0 即关闭该服务。
	// readOnlyPort 是 kubelet 提供服务的只读端口，不带认证/授权。
	// no authentication/authorization.
	// 端口号必须在 1 到 65535（含）之间。
	// 设为 0 表示禁用只读服务。
	// 默认值：0（禁用）
	// +optional
	ReadOnlyPort int32 `json:"readOnlyPort,omitempty"`
	// 未提供证书与私钥时 kubelet 会为对外地址生成自签名证书，适合测试而非生产。
	// tlsCertFile 是 HTTPS 用的 x509 证书文件（若有 CA 证书，接在服务器证书之后）。
	// 若未提供 tlsCertFile 与 tlsPrivateKeyFile，
	// 会为对外地址生成自签名证书与私钥，
	// 并保存到 kubelet --cert-dir 标志指定的目录。
	// 保存到 kubelet --cert-dir 标志指定的目录。
	// 默认值：""
	// +optional
	TLSCertFile string `json:"tlsCertFile,omitempty"`
	// 私钥必须与 tlsCertFile 中的证书配对，否则安全服务无法启动。
	// tlsPrivateKeyFile 是与 tlsCertFile 匹配的 x509 私钥文件。
	// 默认值：""
	// +optional
	TLSPrivateKeyFile string `json:"tlsPrivateKeyFile,omitempty"`
	// TLS 1.3 套件由 Go 内部固定，不受此列表控制。
	// tlsCipherSuites 是服务端允许的加密套件列表。
	// 注意 TLS 1.3 的加密套件不可配置。
	// 取值来自 tls 包常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	// 默认值：nil
	// +optional
	TLSCipherSuites []string `json:"tlsCipherSuites,omitempty"`
	// 列表顺序会被忽略，实际优先级由 Go 的 crypto/tls 决定。
	// tlsCurvePreferences 是服务端允许的密钥交换机制集合，
	// 以 Go crypto/tls CurveID 数值表示。
	// 支持哪些值取决于所用 Go 版本。
	// 各 Go 版本支持的值见 https://pkg.go.dev/crypto/tls#CurveID。
	// 列表顺序被忽略，密钥交换机制由 Go 按内部优先级从此列表中选择。
	// 由 Go 按内部优先级从此列表中选择。
	// 为空时使用 Go 默认曲线。
	// 默认值：nil
	// +optional
	TLSCurvePreferences []int32 `json:"tlsCurvePreferences,omitempty"`
	// 通过限制最低版本来淘汰不安全的旧协议。
	// tlsMinVersion 是支持的最低 TLS 版本。
	// 取值来自 tls 包常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	// 默认值：""
	// +optional
	TLSMinVersion string `json:"tlsMinVersion,omitempty"`
	// 证书轮换依赖 CSR 审批方，无人审批时轮换不会生效。
	// rotateCertificates 启用客户端证书轮换：kubelet 向 certificates.k8s.io API
	// 申请新证书，需要有审批者批准证书签名请求。
	// certificate signing requests.
	// 默认值：false
	// +optional
	RotateCertificates bool `json:"rotateCertificates,omitempty"`
	// 服务端证书引导同样依赖 CSR 审批，且需开启 RotateKubeletServerCertificate 特性。
	// serverTLSBootstrap 启用服务端证书引导：kubelet 不再自签服务证书，
	// 而是向 'certificates.k8s.io' API 申请证书。
	// 需要有审批者批准 CSR；设置该字段时必须启用 RotateKubeletServerCertificate 特性。
	// 需要审批者批准 CSR；设置该字段时必须启用 RotateKubeletServerCertificate 特性
	// 开关。
	// 默认值：false
	// +optional
	ServerTLSBootstrap bool `json:"serverTLSBootstrap,omitempty"`
	// 认证配置决定谁可以调用 kubelet API；默认拒绝匿名请求。
	// authentication 指定 kubelet 服务的请求如何认证。
	// 默认值：
	// anonymous:
	// enabled: false
	// webhook:
	// enabled: true
	// cacheTTL: "2m"
	// +optional
	Authentication KubeletAuthentication `json:"authentication"`
	// 授权配置决定已认证请求能做什么，Webhook 模式借助 apiserver 的 SAR 判定。
	// authorization 指定 kubelet 服务的请求如何授权。
	// 默认值：
	// mode: Webhook
	// webhook:
	// cacheAuthorizedTTL: "5m"
	// cacheUnauthorizedTTL: "30s"
	// +optional
	Authorization KubeletAuthorization `json:"authorization"`
	// 限速既保护镜像仓库也避免节点带宽被打满；0 表示不限制。
	// registryPullQPS 是每秒 registry 拉取次数上限。
	// 该值不能为负数。
	// 设为 0 表示不限制。
	// 默认值：5
	// +optional
	RegistryPullQPS *int32 `json:"registryPullQPS,omitempty"`
	// 突发量只在 QPS 已设置时起作用，用于吸收短时并发拉取。
	// registryBurst 是拉取的突发上限：允许临时突发到此数量，但仍不超过 registryPullQPS。
	// 但仍不超过 registryPullQPS。
	// 该值不能为负数。
	// 仅在 registryPullQPS 大于 0 时有效。
	// 默认值：10
	// +optional
	RegistryBurst int32 `json:"registryBurst,omitempty"`
	// 决定节点上已有镜像是否仍要校验凭据，是安全与可用性的取舍。
	// imagePullCredentialsVerificationPolicy 决定当 Pod 请求节点上已有的镜像时
	// 如何校验凭据：
	// - NeverVerify
	// - 节点上任何人都能使用节点上已有的任何镜像
	// - NeverVerifyPreloadedImages
	// - 由 kubelet 之外的方式拉取到节点上的镜像
	// 无需重新校验拉取凭据即可使用
	// - NeverVerifyAllowlistedImages
	// - 类似 "NeverVerifyPreloadedImages"，但只有
	// `preloadedImagesVerificationAllowlist` 中的节点镜像无需重新校验
	// - AlwaysVerify
	// - 所有镜像都要求重新校验凭据
	// +optional
	ImagePullCredentialsVerificationPolicy ImagePullCredentialsVerificationPolicy `json:"imagePullCredentialsVerificationPolicy,omitempty"`
	// 白名单只接受不含 tag/digest 的镜像 spec，并支持末尾 /* 通配一整段路径。
	// preloadedImagesVerificationAllowlist 指定在 "NeverVerifyAllowlistedImages"
	// 策略下免于凭据重新校验的镜像列表。
	// `imagePullCredentialsVerificationPolicy`.
	// 列表接受完整的路径段通配符后缀 "/*"。
	// 只使用不带 tag 或 digest 的镜像名。
	// +optional
	// +listType=set
	PreloadedImagesVerificationAllowlist []string `json:"preloadedImagesVerificationAllowlist,omitempty"`
	// 事件限速防止应用刷屏把 apiserver 压垮；0 表示不限制。
	// eventRecordQPS 是每秒创建事件数的上限；0 表示不限制。
	// 该值不能为负数。
	// 默认值：50
	// +optional
	EventRecordQPS *int32 `json:"eventRecordQPS,omitempty"`
	// 突发量只在 QPS 大于 0 时生效，不允许为负数。
	// eventBurst 是事件创建的突发上限：允许临时突发到此数量，但仍不超过 eventRecordQPS。
	// 但仍不超过 eventRecordQPS。
	// 该值不能为负数，且仅在 eventRecordQPS > 0 时有效。
	// when eventRecordQPS > 0.
	// 默认值：100
	// +optional
	EventBurst int32 `json:"eventBurst,omitempty"`
	// 这些调试端点（exec/attach/logs/portforward）能访问容器内部，关闭可收紧攻击面。
	// enableDebuggingHandlers 启用日志访问、本地运行容器与命令的服务端点，
	// 包括 exec、attach、logs 和 portforward 功能。
	// 默认值：true
	// +optional
	EnableDebuggingHandlers *bool `json:"enableDebuggingHandlers,omitempty"`
	// 阻塞剖析依赖调试端点开启，单独设置本开关不会生效。
	// enableContentionProfiling 在 enableDebuggingHandlers 为 true 时启用阻塞分析。
	// 默认值：false
	// +optional
	EnableContentionProfiling bool `json:"enableContentionProfiling,omitempty"`
	// 健康端口设为 0 即关闭该端点，通常只监听本机。
	// healthzPort 是本机 healthz 端点的端口（设为 0 表示禁用）。
	// 有效值为 1 到 65535。
	// 默认值：10248
	// +optional
	HealthzPort *int32 `json:"healthzPort,omitempty"`
	// 健康检查默认只绑回环地址，避免健康信息外泄。
	// healthzBindAddress 是 healthz 服务监听的 IP 地址。
	// 默认值："127.0.0.1"
	// +optional
	HealthzBindAddress string `json:"healthzBindAddress,omitempty"`
	// 调低 OOM 分数让 kubelet 在内存紧张时更不容易被内核杀掉，但不可低于 -1000。
	// oomScoreAdj 是 kubelet 进程的 oom-score-adj 值。
	// 取值必须在 [-1000, 1000] 范围内。
	// 默认值：-999
	// +optional
	OOMScoreAdj *int32 `json:"oomScoreAdj,omitempty"`
	// 设置后容器会在主机搜索域之外追加集群域名，用于解析集群内服务。
	// clusterDomain 是本集群的 DNS 域。设置后 kubelet 会让所有容器在宿主机的搜索域之外
	// 再搜索该域。
	// host's search domains.
	// 默认值：""
	// +optional
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// 设置后容器 DNS 解析改用集群 DNS，而不是主机的解析配置。
	// clusterDNS 是集群 DNS 服务器的 IP 地址列表。设置后 kubelet 会让所有容器
	// 用它代替宿主机 DNS 服务器做解析。
	// 代替宿主机 DNS 服务器做解析。
	// 默认值：nil
	// +optional
	ClusterDNS []string `json:"clusterDNS,omitempty"`
	// 已弃用：该字段保留仅为兼容旧配置，不再产生任何效果。
	// streamingConnectionIdleTimeout 是流式连接自动关闭前允许空闲的最长时间。
	// 连接自动关闭前允许空闲的最长时间。
	// Deprecated: no longer has any effect.
	// 默认值："4h"
	// +optional
	StreamingConnectionIdleTimeout metav1.Duration `json:"streamingConnectionIdleTimeout,omitempty"`
	// 这是计算节点状态的频率，实际上报还受 nodeStatusReportFrequency 约束。
	// nodeStatusUpdateFrequency 是 kubelet 计算节点状态并检查是否需要向 API Server
	// 更新的频率。状态在变化时或距上次上报超过 nodeStatusReportFrequency 时
	// 上报给 API Server。
	// 或距上次上报超过 nodeStatusReportFrequency 时。
	// 默认值："10s"
	// +optional
	NodeStatusUpdateFrequency metav1.Duration `json:"nodeStatusUpdateFrequency,omitempty"`
	// 无变化时按此频率上报；检测到变化则立即上报，不等待周期。
	// nodeStatusReportFrequency 是节点状态不变时 kubelet 向 API Server 上报的频率。
	// 只要检测到变化，kubelet 会忽略该频率立即上报。
	// 会忽略该频率并立即上报节点状态，只要检测到任何变化。
	// is detected.
	// nodeStatusReportFrequency 默认值为 5m；但若显式设置了
	// nodeStatusUpdateFrequency，为向后兼容，其默认值会改为
	// 与 nodeStatusUpdateFrequency 相同。
	// nodeStatusUpdateFrequency for backward compatibility.
	// 默认值："5m"
	// +optional
	NodeStatusReportFrequency metav1.Duration `json:"nodeStatusReportFrequency,omitempty"`
	// Lease 过期即视为节点不健康；续约间隔目前固定为 10s，未来可能与租约时长挂钩。
	// nodeLeaseDurationSeconds 是 kubelet 为其对应 Lease 设置的时长。
	// NodeLease 由 kubelet 在 kube-node-lease 命名空间中创建并定期续约，
	// 作为节点健康状态的指示；Lease 过期即可认为节点不健康。
	// Lease 过期即可认为节点不健康。
	// 按 KEP-0009 当前每 10s 续约一次，未来续约间隔可能根据 Lease 时长设置。
	// 未来续约间隔可能根据 Lease 时长设置。
	// 该字段值必须大于 0。
	// 默认值：40
	// +optional
	NodeLeaseDurationSeconds int32 `json:"nodeLeaseDurationSeconds,omitempty"`
	// 未使用镜像至少要存活这么久才会被回收，防止刚拉取就被清理。
	// imageMinimumGCAge 是未使用镜像被垃圾回收前的最短存活时间。
	// garbage collected.
	// 该字段值必须大于 0。
	// 未设置或为 0 时默认为 2m。
	// 默认值："2m"
	// +optional
	ImageMinimumGCAge metav1.Duration `json:"imageMinimumGCAge,omitempty"`
	// 为 0 表示不按闲置时长回收镜像，避免节点磁盘被旧镜像填满需另行配置。
	// imageMaximumGCAge 是未使用镜像被垃圾回收前的最长存活时间。
	// 默认值为 "0s"，表示禁用该字段，即不会因镜像长时间未使用而回收。
	// 即不会因镜像长时间未使用而回收。
	// 默认值："0s"（禁用）
	// +optional
	ImageMaximumGCAge metav1.Duration `json:"imageMaximumGCAge,omitempty"`
	// 磁盘用量超过该水位必定触发镜像 GC，且必须高于低水位。
	// imageGCHighThresholdPercent 是触发镜像垃圾回收的磁盘使用率阈值。
	// 百分比由该字段值除以 100 得出，因此必须在 0 到 100（含）之间。
	// 因此必须在 0 到 100（含）之间。指定时必须大于
	// 指定时必须大于 imageGCLowThresholdPercent。
	// imageGCLowThresholdPercent.
	// 默认值：85
	// +optional
	ImageGCHighThresholdPercent *int32 `json:"imageGCHighThresholdPercent,omitempty"`
	// GC 会把磁盘用量降到该水位以下；必须低于高水位。
	// imageGCLowThresholdPercent 是不触发镜像垃圾回收的磁盘使用率下限。
	// 即回收到的最低磁盘使用率。百分比由字段值除以 100 得出，
	// 因此必须在 0 到 100（含）之间；指定时必须小于
	// imageGCHighThresholdPercent。
	// 默认值：80
	// +optional
	ImageGCLowThresholdPercent *int32 `json:"imageGCLowThresholdPercent,omitempty"`
	// 统计周期越短越贴近实时，但会增加卷插件的调用频率。
	// volumeStatsAggPeriod 是计算并缓存所有 Pod 卷磁盘用量的频率。
	// 所有 Pod 卷磁盘用量的频率。
	// 默认值："1m"
	// +optional
	VolumeStatsAggPeriod metav1.Duration `json:"volumeStatsAggPeriod,omitempty"`
	// 把 kubelet 自身放进独立 cgroup，便于限制其资源占用。
	// kubeletCgroups 是隔离 kubelet 所在 cgroup 的绝对路径名。
	// 默认值：""
	// +optional
	KubeletCgroups string `json:"kubeletCgroups,omitempty"`
	// 回滚该设置需要重启节点；非空时还必须指定 cgroupRoot。
	// systemCgroups 是放置所有尚未在容器中的非内核进程的 cgroup 绝对路径名。
	// 为空表示不放入容器。回滚该标志需要重启。
	// 回滚该标志需要重启。
	// 该字段非空时必须指定 cgroupRoot。
	// 默认值：""
	// +optional
	SystemCgroups string `json:"systemCgroups,omitempty"`
	// 运行时尽力而为地遵守该根 cgroup，并非所有运行时都能完全落实。
	// cgroupRoot 是 Pod 使用的根 cgroup，由容器运行时尽力处理。
	// 由容器运行时尽力处理。
	// +optional
	CgroupRoot string `json:"cgroupRoot,omitempty"`
	// 按 QoS 分级建 cgroup 是 Node Allocatable 等能力的基础。
	// cgroupsPerQOS 启用基于 QoS 的 CGroup 层级：为各 QoS 类建顶层 CGroup，
	// 所有 Burstable 与 BestEffort Pod 放在各自的顶层 QoS CGroup 下。
	// QoS CGroup.
	// 默认值：true
	// +optional
	CgroupsPerQOS *bool `json:"cgroupsPerQOS,omitempty"`
	// 需与容器运行时使用的 driver 一致，否则会出现资源视图分裂。
	// cgroupDriver 是 kubelet 操作主机 CGroup 的驱动（cgroupfs 或 systemd）。
	// or systemd).
	// 默认值："cgroupfs"
	// +optional
	CgroupDriver string `json:"cgroupDriver,omitempty"`
	// CPU Manager 策略名，决定 CPU 独占分配的行为。
	// cpuManagerPolicy 是要使用的策略名。
	// 默认值："None"
	// +optional
	CPUManagerPolicy string `json:"cpuManagerPolicy,omitempty"`
	// cgroup v2 默认按组 OOM kill；设为 true 恢复与 v1 一致的逐进程行为。
	// singleProcessOOMKill 为 true 时，阻止在 cgroups v2 中给容器 cgroup 设置 `memory.oom.group`，
	// 使容器内进程被逐个 OOM 杀死而非整组杀死；为 true 时行为与 cgroups v1 一致。
	// 为 true 时行为与 cgroups v1 一致。
	// 未指定时默认值自动决定。
	// 非 Linux（如 Windows）上只允许 null/缺省。
	// cgroup v1 Linux 上只允许 null/缺省与 true。
	// cgroup v2 Linux 上允许 null/缺省、true 与 false；默认值为 false。
	// +optional
	SingleProcessOOMKill *bool `json:"singleProcessOOMKill,omitempty"`
	// 给 CPU Manager 策略传递细调参数，具体键由所选策略决定。
	// cpuManagerPolicyOptions 是一组 key=value，用于设置额外选项微调 CPU 管理器策略行为。
	// 以微调 CPU 管理器策略行为。
	// 默认值：nil
	// +optional
	CPUManagerPolicyOptions map[string]string `json:"cpuManagerPolicyOptions,omitempty"`
	// 对账周期决定新增独占 CPU 分配的生效延迟。
	// cpuManagerReconcilePeriod 是 CPU 管理器的对账周期。
	// 默认值："10s"
	// +optional
	CPUManagerReconcilePeriod metav1.Duration `json:"cpuManagerReconcilePeriod,omitempty"`
	// 需要 MemoryManager 特性门控开启；静态策略才会做 NUMA 内存绑定。
	// memoryManagerPolicy 是内存管理器使用的策略名。
	// 需要启用 MemoryManager 特性开关。
	// 默认值："none"
	// +optional
	MemoryManagerPolicy string `json:"memoryManagerPolicy,omitempty"`
	// 策略越严格越能保证 NUMA 本地性，但也更容易拒绝 Pod 准入。
	// topologyManagerPolicy 是拓扑管理器策略名。合法值包括：
	// Valid values include:
	//
	// - `restricted`：kubelet 只接收所请求资源 NUMA 对齐最优的 Pod；
	//   requested resources;
	// - `best-effort`：kubelet 倾向接收 CPU 与设备资源 NUMA 对齐的 Pod；
	//   resources;
	// - `none`：kubelet 不感知 Pod CPU 与设备资源的 NUMA 对齐情况；
	// - `single-numa-node`：kubelet 只接收 CPU 与设备资源位于单个 NUMA 节点上的 Pod。
	//   of CPU and device resources.
	//
	// 默认值："none"
	// +optional
	TopologyManagerPolicy string `json:"topologyManagerPolicy,omitempty"`
	// 按 Pod 对齐更宽松，按容器对齐更精确，按场景选择。
	// topologyManagerScope 表示拓扑管理器请求、提示提供者生成提示的作用域。合法值包括：
	// 拓扑管理器请求、提示提供者生成提示的作用域。合法值包括：
	//
	// - `container`：拓扑策略按容器粒度应用；
	// - `pod`：拓扑策略按 Pod 粒度应用。
	//
	// 默认值："container"
	// +optional
	TopologyManagerScope string `json:"topologyManagerScope,omitempty"`
	// 需同时开启 TopologyManager 与 TopologyManagerPolicyOptions 两个特性门控。
	// TopologyManagerPolicyOptions 是一组 key=value，用于微调拓扑管理器策略行为。
	// 以微调拓扑管理器策略行为。
	// 需要同时启用 "TopologyManager" 与 "TopologyManagerPolicyOptions" 特性开关。
	// 默认值：nil
	// +optional
	TopologyManagerPolicyOptions map[string]string `json:"topologyManagerPolicyOptions,omitempty"`
	// 为 Guaranteed 级别预留资源比例，需 QOSReserved 特性门控开启。
	// qosReserved 是资源名到百分比的映射，指定为 guaranteed QoS 层级独占保留的资源最低比例。
	// 为 guaranteed QoS 层级独占保留的资源最低比例。
	// guaranteed QoS tier.
	// 当前仅支持 "memory"。
	// 需要启用 QOSReserved 特性开关。
	// 默认值：nil
	// +optional
	QOSReserved map[string]string `json:"qosReserved,omitempty"`
	// 仅适用于短请求；拉取镜像、日志、exec/attach 等长请求不受此限制。
	// runtimeRequestTimeout 是除长请求（拉取、日志、exec、attach）之外所有运行时请求的超时。
	// （拉取、日志、exec、attach）之外。
	// 默认值："2m"
	// +optional
	RuntimeRequestTimeout metav1.Duration `json:"runtimeRequestTimeout,omitempty"`
	// promiscuous-bridge 假定存在名为 cbr0 的网桥；多数场景应使用 hairpin-veth。
	// hairpinMode 指定 kubelet 如何为发夹报文配置容器网桥。
	// 设置后，Service 端点访问自身服务时可负载均衡回自己。取值：
	// 设置后，Service 端点访问自身服务时
	// 可负载均衡回自己。取值：
	//
	// - "promiscuous-bridge"：把容器网桥设为混杂模式；
	// - "hairpin-veth"：在容器 veth 接口上设置 hairpin 标志；
	// - "none"：不处理。
	//
	// 通常必须设置 `--hairpin-mode=hairpin-veth` 才能实现发夹 NAT，
	// 因为 promiscuous-bridge 假定存在名为 cbr0 的容器网桥。
	// 默认值："promiscuous-bridge"
	// +optional
	HairpinMode string `json:"hairpinMode,omitempty"`
	// 限制单节点 Pod 数量，防止节点过载。
	// maxPods 是该 kubelet 上可运行的最大 Pod 数。
	// 该值必须为非负整数。
	// 默认值：110
	// +optional
	MaxPods int32 `json:"maxPods,omitempty"`
	// 仅在独立模式生效；集群模式下由控制面分配。
	// podCIDR 是 Pod IP 地址使用的 CIDR，仅在 standalone 模式下使用。
	// 集群模式下由控制面提供。
	// 默认值：""
	// +optional
	PodCIDR string `json:"podCIDR,omitempty"`
	// 限制单个 Pod 的进程数，防止 fork 炸弹耗尽节点 PID。
	// podPidsLimit 是单个 Pod 中 PID 数的上限。
	// 默认值：-1
	// +optional
	PodPidsLimit *int64 `json:"podPidsLimit,omitempty"`
	// 设为空字符串会关闭 DNS 查找，除非有意为之否则不要这么做。
	// resolvConf 是作为容器 DNS 解析配置基础的解析器配置文件。
	// 作为容器 DNS 解析配置的基础。
	// 设为空字符串会覆盖默认值，实际上禁用 DNS 查询。
	// 默认值："/etc/resolv.conf"
	// +optional
	ResolverConfig *string `json:"resolvConf,omitempty"`
	// 一次性模式，拉取一次 Pod 后退出，主要用于调试。
	// runOnce 让 kubelet 向 API Server 检查一次 Pod，
	// 运行这些 Pod 以及静态 Pod 文件中的 Pod，然后退出。
	// 默认值：false
	// +optional
	RunOnce bool `json:"runOnce,omitempty"`
	// 关闭后容器 CPU limit 将不再通过 CFS quota 强制执行。
	// cpuCFSQuota 对指定了 CPU limit 的容器启用 CPU CFS 配额限制。
	// specify CPU limits.
	// 默认值：true
	// +optional
	CPUCFSQuota *bool `json:"cpuCFSQuota,omitempty"`
	// 需 CustomCPUCFSQuotaPeriod 特性门控开启；普通场景不应修改默认值。
	// cpuCFSQuotaPeriod 是 CPU CFS 配额周期值 `cpu.cfs_period_us`。
	// 该值必须在 1 毫秒到 1 秒（含）之间。
	// 需要启用 CustomCPUCFSQuotaPeriod 特性开关。
	// 默认值："100ms"
	// +optional
	CPUCFSQuotaPeriod *metav1.Duration `json:"cpuCFSQuotaPeriod,omitempty"`
	// -1 表示不限制，0 表示不返回镜像；限制可减小 Node 状态体积。
	// nodeStatusMaxImages 限制 Node.status.images 中上报的镜像数量。
	// 该值必须大于 -2。
	// 注意：-1 表示不限制；0 表示不返回镜像。
	// 默认值：50
	// +optional
	NodeStatusMaxImages *int32 `json:"nodeStatusMaxImages,omitempty"`
	// 提高文件描述符上限以支持大量 Pod，但受系统 ulimit 约束。
	// maxOpenFiles 是 kubelet 进程可打开的文件数。
	// 该值必须为非负数。
	// 默认值：1000000
	// +optional
	MaxOpenFiles int64 `json:"maxOpenFiles,omitempty"`
	// 决定与 apiserver 通信的序列化格式，protobuf 通常比 JSON 更高效。
	// contentType 是发往 apiserver 的请求的内容类型。
	// 默认值："application/vnd.kubernetes.protobuf"
	// +optional
	ContentType string `json:"contentType,omitempty"`
	// 保护 apiserver 免受单节点突发请求冲击。
	// kubeAPIQPS 是与 kubernetes apiserver 通信时使用的 QPS。
	// 默认值：50
	// +optional
	KubeAPIQPS *int32 `json:"kubeAPIQPS,omitempty"`
	// 突发量配合 QPS 使用，不允许为负数。
	// kubeAPIBurst 是与 kubernetes apiserver 通信时允许的突发量。
	// 该字段不能为负数。
	// 默认值：100
	// +optional
	KubeAPIBurst int32 `json:"kubeAPIBurst,omitempty"`
	// 老版本 Docker/Aufs 后端并发拉取不稳定，这些环境不要改为并发。
	// serializeImagePulls 启用时让 kubelet 一次只拉取一个镜像。
	// 建议在运行 docker daemon < 1.9 或使用 Aufs 存储后端的节点上
	// 不要更改默认值。
	// 详见 issue #10959。
	// 默认值：true
	// +optional
	SerializeImagePulls *bool `json:"serializeImagePulls,omitempty"`
	// 与 SerializeImagePulls 互斥；不设置表示不限制并发数。
	// MaxParallelImagePulls 设置并行拉取镜像的最大数量。
	// SerializeImagePulls 为 true 时不能设置该字段。
	// 设为 nil 表示不限制。
	// 默认值：nil
	// +optional
	MaxParallelImagePulls *int32 `json:"maxParallelImagePulls,omitempty"`
	// 硬驱逐不设宽限期，达到阈值立即驱逐 Pod。
	// evictionHard 是信号名到数量的映射，定义硬驱逐阈值。
	// 例如：`{"memory.available": "300Mi"}`。
	// 要显式禁用，可对任意资源设置 0% 或 100% 阈值。
	// 默认值：
	// memory.available:  "100Mi"
	// nodefs.available:  "10%"
	// nodefs.inodesFree: "5%"
	// imagefs.available: "15%"
	// +optional
	EvictionHard map[string]string `json:"evictionHard,omitempty"`
	// 软驱逐允许配置宽限期，给 Pod 留出优雅退出时间。
	// evictionSoft 是信号名到数量的映射，定义软驱逐阈值。
	// 例如：`{"memory.available": "300Mi"}`。
	// 默认值：nil
	// +optional
	EvictionSoft map[string]string `json:"evictionSoft,omitempty"`
	// 宽限期配合软驱逐阈值使用，避免短暂抖动就驱逐 Pod。
	// evictionSoftGracePeriod 是信号名到时长的映射，定义各软驱逐信号的宽限期。
	// 例如：`{"memory.available": "30s"}`。
	// 默认值：nil
	// +optional
	EvictionSoftGracePeriod map[string]string `json:"evictionSoftGracePeriod,omitempty"`
	// 设置等待窗口防止压力状态在阈值附近反复震荡。
	// evictionPressureTransitionPeriod 是 kubelet 退出驱逐压力状态前必须等待的时长。
	// 0s 会被转换为默认值 5m。
	// 默认值："5m"
	// +optional
	EvictionPressureTransitionPeriod metav1.Duration `json:"evictionPressureTransitionPeriod,omitempty"`
	// 软驱逐时对 Pod 终止宽限期的上限，防止个别 Pod 拖长驱逐过程。
	// evictionMaxPodGracePeriod 是满足软驱逐阈值时终止 Pod 允许的最大宽限期（秒）。
	// 它实际上限制了软驱逐期间 Pod 的 terminationGracePeriodSeconds。
	// 默认值：0
	// +optional
	EvictionMaxPodGracePeriod int32 `json:"evictionMaxPodGracePeriod,omitempty"`
	// 一次驱逐至少回收这么多资源，避免频繁触发小动作。
	// evictionMinimumReclaim 是信号名到数量的映射，定义最小回收量，
	// 即资源紧张时 kubelet 驱逐 Pod 会回收的该资源的最小量。
	// 即资源紧张时 kubelet 驱逐 Pod 会回收的该资源最小量。
	// 例如：`{"imagefs.available": "2Gi"}`。
	// 默认值：nil
	// +optional
	EvictionMinimumReclaim map[string]string `json:"evictionMinimumReclaim,omitempty"`
	// 为 true 时用户显式配置的信号优先，其余信号继承默认值。
	// mergeDefaultEvictionSettings 表示 evictionHard、evictionSoft、
	// evictionSoftGracePeriod 与 evictionMinimumReclaim 的默认值应与本配置中
	// 指定值合并。本配置中指定的信号优先，未指定的继承默认值。
	// 本配置中未指定的信号继承默认值。
	// 为 false 时，若本配置指定了任一信号，其它未指定的信号会被设为 0。
	// 其它未指定的信号会被设为 0。
	// 该合并只适用于有默认值的字段，当前只有 evictionHard 有默认值。
	// 默认值：false
	// +optional
	MergeDefaultEvictionSettings *bool `json:"mergeDefaultEvictionSettings,omitempty"`
	// 与 maxPods 共同生效，最终取两者中更严格的上限。
	// podsPerCore 是每核最大 Pod 数，不能超过 maxPods。
	// 该值必须为非负整数。
	// 为 0 表示不限制 Pod 数。
	// 默认值：0
	// +optional
	PodsPerCore int32 `json:"podsPerCore,omitempty"`
	// CSI 卷的挂载/卸载只能由控制器完成，因此这类场景必须开启此选项。
	// enableControllerAttachDetach 让 Attach/Detach 控制器管理调度到本节点的卷挂载/卸载，
	// 并禁止 kubelet 自己执行挂载/卸载操作。
	// 注意：kubelet 不支持挂载/卸载 CSI 卷，该场景需要此选项为 true。
	// 因此该场景需要此选项为 true。
	// 默认值：true
	// +optional
	EnableControllerAttachDetach *bool `json:"enableControllerAttachDetach,omitempty"`
	// 开启后内核参数不符合预期则直接报错退出，而不是擅自修改。
	// protectKernelDefaults 为 true 时，若内核参数与预期不符 kubelet 会报错；
	// 否则会尝试修改内核参数使其符合预期。
	// 默认值：false
	// +optional
	ProtectKernelDefaults bool `json:"protectKernelDefaults,omitempty"`
	// 创建链作为提示，便于其他组件感知本节点的 iptables 配置。
	// makeIPTablesUtilChains 为 true 时，kubelet 会在 iptables 中创建 KUBE-IPTABLES-HINT 链，
	// 作为提示告知其它组件系统上的 iptables 配置。
	// configuration of iptables on the system.
	// 默认值：true
	// +optional
	MakeIPTablesUtilChains *bool `json:"makeIPTablesUtilChains,omitempty"`
	// 已弃用：保留字段仅为兼容旧配置，不再产生任何效果。
	// iptablesMasqueradeBit 曾用于控制 KUBE-MARK-MASQ 链的创建。
	// chain.
	// Deprecated: no longer has any effect.
	// 默认值：14
	// +optional
	IPTablesMasqueradeBit *int32 `json:"iptablesMasqueradeBit,omitempty"`
	// 已弃用：保留字段仅为兼容旧配置，不再产生任何效果。
	// iptablesDropBit 曾用于控制 KUBE-MARK-DROP 链的创建。
	// Deprecated: no longer has any effect.
	// 默认值：15
	// +optional
	IPTablesDropBit *int32 `json:"iptablesDropBit,omitempty"`
	// 可逐项覆盖编译进二进制的门控默认值；拼错的特性名会导致启动失败。
	// featureGates 是特性名到布尔值的映射，用于启用/禁用实验性特性。
	// 该字段对 "k8s.io/kubernetes/pkg/features/kube_features.go" 中的内置默认值
	// 做局部覆盖。
	// 默认值：nil
	// +optional
	FeatureGates map[string]bool `json:"featureGates,omitempty"`
	// swap 会让内存 QoS 判定失准，因此默认禁止在开启 swap 的节点上启动。
	// failSwapOn 让 kubelet 在节点启用 swap 时启动失败。
	// 默认值：true
	// +optional
	FailSwapOn *bool `json:"failSwapOn,omitempty"`
	// 需 NodeSwap 特性门控开启，且 failSwapOn 通常要一并关闭。
	// memorySwap 配置容器工作负载可用的 swap 内存。
	// +featureGate=NodeSwap
	// +optional
	MemorySwap MemorySwapConfiguration `json:"memorySwap,omitempty"`
	// 控制单个日志文件的大小，超过即轮转。
	// containerLogMaxSize 是容器日志文件轮转前的最大大小，如 "5Mi" 或 "256Ki"。
	// file before it is rotated. For example: "5Mi" or "256Ki".
	// 默认值："10Mi"
	// +optional
	ContainerLogMaxSize string `json:"containerLogMaxSize,omitempty"`
	// 限制轮转后保留的日志文件数量，避免磁盘被日志占满。
	// containerLogMaxFiles 指定一个容器可存在的日志文件最大数量。
	// be present for a container.
	// 默认值：5
	// +optional
	ContainerLogMaxFiles *int32 `json:"containerLogMaxFiles,omitempty"`

	// 并发轮转可加快大量容器的日志处理；设为 1 退化为串行。
	// ContainerLogMaxWorkers 指定执行日志轮转的最大并发 worker 数。
	// 设为 1 可禁用并发日志轮转流程。
	// concurrent log rotation workflows
	// 默认值：1
	// +optional
	ContainerLogMaxWorkers *int32 `json:"containerLogMaxWorkers,omitempty"`

	// 日志产生快时可调小间隔，让轮转更及时。
	// ContainerLogMonitorInterval 指定监控容器日志以执行轮转的间隔。
	// 默认为 10 * time.Second，可根据日志生成速率与所需轮转大小
	// 自定义为更小的值。
	// rotated against
	// 默认值：10s
	// +optional
	ContainerLogMonitorInterval *metav1.Duration `json:"containerLogMonitorInterval,omitempty"`
	// Watch 模式及时且省请求，是默认选择；Get/Cache 适用于受限环境。
	// configMapAndSecretChangeDetectionStrategy 是 ConfigMap 与 Secret 管理器的运行模式。合法值包括：
	// 是 ConfigMap 与 Secret 管理器的运行模式。合法值包括：
	//
	// - `Get`：kubelet 直接从 API Server 拉取所需对象；
	// - `Cache`：kubelet 对拉取的对象使用 TTL 缓存；
	// - `Watch`：kubelet 通过 watch 监听关注对象的变化。
	//
	// 默认值："Watch"
	// +optional
	ConfigMapAndSecretChangeDetectionStrategy ResourceChangeDetectionStrategy `json:"configMapAndSecretChangeDetectionStrategy,omitempty"`

	/* the following fields are meant for Node Allocatable */

	// 为系统守护进程预留资源，避免 Pod 挤占导致节点失稳。
	// systemReserved 是 ResourceName=ResourceQuantity（如 cpu=200m,memory=150G）映射，
	// 描述为非 Kubernetes 组件保留的资源。当前仅支持 cpu 与 memory。
	// 当前仅支持 cpu 与 memory。
	// 详见 https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources。
	// 默认值：nil
	// +optional
	SystemReserved map[string]string `json:"systemReserved,omitempty"`
	// 为 kubelet、容器运行时等 Kubernetes 组件预留资源。
	// kubeReserved 是 ResourceName=ResourceQuantity（如 cpu=200m,memory=150G）映射，
	// 描述为 Kubernetes 系统组件保留的资源。
	// 当前支持 cpu、memory 与根文件系统的本地存储。
	// See https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources
	// 详见 https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources。
	// 默认值：nil
	// +optional
	KubeReserved map[string]string `json:"kubeReserved,omitempty"`
	// 静态 CPU 列表比按比例预留更确定，但不能与两个 reserved cgroup 选项同时使用。
	// reservedSystemCPUs 指定为主机级系统线程与 Kubernetes 相关线程保留的 CPU 列表，
	// 相比 systemReserved/kubeReserved 的"动态"列表，它提供"静态" CPU 列表。
	// CPU list rather than the "dynamic" list by systemReserved and kubeReserved.
	// 该选项不支持 systemReservedCgroup 或 kubeReservedCgroup。
	ReservedSystemCPUs string `json:"reservedSystemCPUs,omitempty"`
	// 只允许填写上一个 minor 版本，目的是给升级者一个观察指标下线的窗口。
	// showHiddenMetricsForVersion 是想显示隐藏指标的历史版本。
	// 只有上一个次版本有意义，其它值不允许。
	// 格式为 `<major>.<minor>`，例如 `1.16`。
	// The format is `<major>.<minor>`, e.g.: `1.16`.
	// 使用此格式是为了让你有机会注意到下个版本隐藏了更多指标，
	// 而不是在再下个版本永久删除时才措手不及。
	// 默认值：""
	// +optional
	ShowHiddenMetricsForVersion string `json:"showHiddenMetricsForVersion,omitempty"`
	// 配合 enforceNodeAllocatable 中的 system-reserved 选项使用，否则预留不会被执行。
	// systemReservedCgroup 帮助 kubelet 识别顶层 CGroup 的绝对名字，
	// 用于对 OS 系统守护进程强制 `systemReserved` 资源预留。
	// Refer to [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)
	// 详见 [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)。
	// Default: ""
	// +optional
	SystemReservedCgroup string `json:"systemReservedCgroup,omitempty"`
	// 配合 enforceNodeAllocatable 中的 kube-reserved 选项使用，否则预留不会被执行。
	// kubeReservedCgroup 帮助 kubelet 识别顶层 CGroup 的绝对名字，
	// 用于对 Kubernetes 节点系统守护进程强制 `KubeReserved` 资源预留。
	// Refer to [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)
	// 详见 [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)。
	// Default: ""
	// +optional
	KubeReservedCgroup string `json:"kubeReservedCgroup,omitempty"`
	// 决定节点预留是否真正落到 cgroup 上；仅在 cgroupsPerQOS 开启时有效。
	// 该标志指定 kubelet 要执行的各种 Node Allocatable 强制措施。
	// 接受选项列表，合法值为 `none`、`pods`、`system-reserved`、
	// `system-reserved-compressible`、`kube-reserved`、`kube-reserved-compressible`。
	// 指定 `none` 时不能再指定其它选项。
	// 列表中有 `system-reserved` 时必须指定 systemReservedCgroup。
	// 列表中有 `kube-reserved` 时必须指定 kubeReservedCgroup。
	// 指定了 `compressible` 选项时不能再指定对应的非 compressible 选项。
	// 该字段仅在 `cgroupsPerQOS` 为 true 时支持。
	// Refer to [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)
	// 详见 [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)。
	// Default: ["pods"]
	// +optional
	EnforceNodeAllocatable []string `json:"enforceNodeAllocatable,omitempty"`
	// 默认禁止不安全 sysctl，白名单需逐项评估对节点隔离性的影响。
	// 以逗号分隔的不安全 sysctl 或 sysctl 模式（以 `*` 结尾）白名单。
	// 不安全 sysctl 组包括 `kernel.shm*`、`kernel.msg*`、`kernel.sem`、`fs.mqueue.*`
	// 和 `net.*`，例如 "`kernel.msg*,net.ipv4.route.min_pmtu`"。
	// 默认值：[]
	// +optional
	AllowedUnsafeSysctls []string `json:"allowedUnsafeSysctls,omitempty"`
	// Pod 自己声明的 sysctl 会覆盖这里的默认值，便于集群统一设置基线。
	// DefaultPodSysctls 是一组会应用到所有 Pod 的默认 sysctl。
	// 可被 Pod spec.securityContext.sysctls 中设置的 sysctl 覆盖。
	// 支持有命名空间的组：`kernel.shm*`、`kernel.msg*`、`kernel.sem`、`fs.mqueue.*`、`net.*`、`kernel.domainname` 与 `user.*`。
	// 例如：{"net.ipv4.ip_forward": "1", "kernel.shmall": "1048576"}
	// +optional
	DefaultPodSysctls map[string]string `json:"defaultPodSysctls,omitempty"`
	// 用于发现第三方 in-tree 卷插件的目录。
	// volumePluginDir 是搜索额外第三方卷插件的目录的完整路径。
	// 搜索额外第三方卷插件的目录的完整路径。
	// 默认值："/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"
	// +optional
	VolumePluginDir string `json:"volumePluginDir,omitempty"`
	// 云厂商用它把 Node 对象映射到具体的云主机实例。
	// providerID 若设置，则指定外部 provider（如云厂商）用来标识特定节点的唯一 ID。
	// 外部 provider（如云厂商）用来标识特定节点的唯一 ID。
	// 默认值：""
	// +optional
	ProviderID string `json:"providerID,omitempty"`
	// 内核通知可减少轮询开销，但需要内核与 cgroup 版本支持。
	// kernelMemcgNotification 若设置，指示 kubelet 用内核 memcg 通知来判断是否超过内存驱逐阈值，
	// 而不是轮询。
	// exceeded rather than polling.
	// 默认值：false
	// +optional
	KernelMemcgNotification bool `json:"kernelMemcgNotification,omitempty"`
	// 复用 component-base 的日志配置，保持各组件日志行为一致。
	// logging 指定日志选项。
	// 详见 [Logs Options](https://github.com/kubernetes/component-base/blob/master/logs/options.go)。
	// for more information.
	// 默认值：
	// Format: text
	// + optional
	Logging logsapi.LoggingConfiguration `json:"logging,omitempty"`
	// 通过 /logs 暴露节点日志，开启前应确认没有敏感信息泄漏风险。
	// enableSystemLogHandler 通过 web 接口 host:port/logs/ 启用系统日志。
	// 默认值：true
	// +optional
	EnableSystemLogHandler *bool `json:"enableSystemLogHandler,omitempty"`
	// 节点日志查询可能暴露敏感内容，建议仅在排障期间临时开启。
	// enableSystemLogQuery 在 /logs 端点启用节点日志查询功能。
	// 还需同时启用 EnableSystemLogHandler 才能生效。
	// 启用此功能有安全影响，建议仅在调试需要时启用，其余时间关闭。
	// purposes and disabling otherwise.
	// 默认值：false
	// +optional
	EnableSystemLogQuery *bool `json:"enableSystemLogQuery,omitempty"`
	// 节点关机时留给 Pod 优雅退出的总时长，需 GracefulNodeShutdown 特性门控。
	// shutdownGracePeriod 指定节点关闭时应延迟关机的总时长，以及 Pod 终止的总宽限期。
	// 以及节点关闭时 Pod 终止的总宽限期。
	// 默认值："0s"
	// +featureGate=GracefulNodeShutdown
	// +optional
	ShutdownGracePeriod metav1.Duration `json:"shutdownGracePeriod,omitempty"`
	// 必须小于总关机宽限期，余下时间留给普通 Pod。
	// shutdownGracePeriodCriticalPods 指定节点关闭时终止关键 Pod 的时长，应小于 shutdownGracePeriod。
	// 应小于 shutdownGracePeriod。
	// 例如 shutdownGracePeriod=30s、shutdownGracePeriodCriticalPods=10s 时，
	// 节点关闭前 20 秒用于优雅终止普通 Pod，最后 10 秒用于终止关键 Pod。
	// 最后 10 秒用于终止关键 Pod。
	// critical pods.
	// 默认值："0s"
	// +featureGate=GracefulNodeShutdown
	// +optional
	ShutdownGracePeriodCriticalPods metav1.Duration `json:"shutdownGracePeriodCriticalPods,omitempty"`
	// 按优先级细分关机宽限期，需 GracefulNodeShutdownBasedOnPodPriority 特性门控。
	// shutdownGracePeriodByPodPriority 按 Pod 的优先级类值指定关闭宽限期。
	// 收到关机请求时，kubelet 会根据 Pod 优先级以相应宽限期启动节点上所有 Pod 的关闭，
	// 然后等待全部 Pod 退出。
	// kubelet 会根据 Pod 优先级以相应宽限期启动关闭，
	// 然后等待所有 Pod 退出。
	// 数组中每项表示优先级处于该项值与下一更高项值之间的 Pod 的优雅关闭时间。
	// 优先级处于该项值与下一更高项之间的 Pod 的
	// 优雅关闭时间。
	// 例如：关键 Pod 10s、priority>=10000 的 Pod 20s、其余 Pod 30s。
	// 其余 Pod 30s。
	//
	// shutdownGracePeriodByPodPriority:
	// - priority: 2000000000
	// shutdownGracePeriodSeconds: 10
	// - priority: 10000
	// shutdownGracePeriodSeconds: 20
	// - priority: 0
	// shutdownGracePeriodSeconds: 30
	//
	// kubelet 退出前等待的时间最多为节点上各优先级区间最大 shutdownGracePeriodSeconds 的值。
	// 即节点上各优先级区间最大 shutdownGracePeriodSeconds 的值。
	// 所有 Pod 退出或达到宽限期后，kubelet 释放关机抑制锁。
	// the shutdown inhibit lock.
	// 需要启用 GracefulNodeShutdown 特性开关。
	// 若设置了 ShutdownGracePeriod 或 ShutdownGracePeriodCriticalPods，该配置必须为空。
	// 默认值：nil
	// +featureGate=GracefulNodeShutdownBasedOnPodPriority
	// +optional
	ShutdownGracePeriodByPodPriority []ShutdownGracePeriodByPodPriority `json:"shutdownGracePeriodByPodPriority,omitempty"`
	// 需 KubeletCrashLoopBackOffMax 特性门控开启才生效。
	// CrashLoopBackOff 包含修改容器重启行为的节点级参数配置。
	// container restart behavior
	// +featureGate=KubeletCrashLoopBackOffMax
	// +optional
	CrashLoopBackOff CrashLoopBackOffConfig `json:"crashLoopBackOff,omitempty"`
	// 为 NUMA 节点预留内存，避免内存管理器把系统所需内存分配给容器。
	// reservedMemory 指定 NUMA 节点内存预留列表（逗号分隔）。
	// 该参数仅在内存管理器特性上下文中有意义。
	// 内存管理器不会把预留内存分配给容器工作负载。
	// 例如 NUMA0 有 10Gi 内存，且预留 1Gi，
	// 内存管理器会认为只有 9Gi 可分配。
	// only 9Gi is available for allocation.
	// 可指定不同 NUMA 节点与内存类型。
	// 该参数可完全省略，但应注意所有 NUMA 节点的预留内存总量
	// 应等于 [node allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable) 指定的内存量。
	// by the [node allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable).
	// 若至少一个 node allocatable 参数非零，则必须至少指定一个 NUMA 节点。
	// 则必须至少指定一个 NUMA 节点。
	// 同时应避免：
	//
	// 1. 重复的同一 NUMA 节点与内存类型但值不同；
	// 2. 任何内存类型的零限制；
	// 3. 机器上不存在的 NUMA 节点 ID；
	// 4. memory 与 hugepages-<size> 之外的内存类型。
	//
	// 默认值：nil
	// +optional
	ReservedMemory []MemoryReservation `json:"reservedMemory,omitempty"`
	// pprof 端点会暴露运行时细节，生产环境应谨慎开放。
	// enableProfilingHandler 通过 web 接口 host:port/debug/pprof/ 启用性能分析。
	// 默认值：true
	// +optional
	EnableProfilingHandler *bool `json:"enableProfilingHandler,omitempty"`
	// 暴露命令行 flag 的调试端点，便于核对生效配置。
	// enableDebugFlagsHandler 通过 web 接口 host:port/debug/flags/v 启用标志端点。
	// 默认值：true
	// +optional
	EnableDebugFlagsHandler *bool `json:"enableDebugFlagsHandler,omitempty"`
	// 开启后未声明 seccomp 的负载默认使用 RuntimeDefault profile。
	// SeccompDefault 启用 `RuntimeDefault` 作为所有工作负载的默认 seccomp profile。
	// 默认值：false
	// +optional
	SeccompDefault *bool `json:"seccompDefault,omitempty"`
	// 因子越小内存回收压力越大；为 nil 时不设置 memory.high。
	// MemoryThrottlingFactor 是设置 cgroupv2 memory.high 以实施 MemoryQoS 时，
	// 乘以内存 limit 或节点可分配内存的系数。
	// 减小该值会降低容器 cgroup 的 high 限制并加大回收压力，
	// 增大则回收压力更小；为 nil 时不设置 memory.high。
	// 详见 https://kep.k8s.io/2570。
	// 默认值：nil
	// +featureGate=MemoryQoS
	// +optional
	MemoryThrottlingFactor *float64 `json:"memoryThrottlingFactor,omitempty"`
	// 需 MemoryQoS 特性门控开启；默认 None 避免锁死内存影响节点稳定。
	// MemoryReservationPolicy 控制 kubelet 如何应用 cgroup v2 内存保护。
	// "None"（默认）：kubelet 不为容器和 Pod 设置 memory.min，
	// 确保内核不锁定硬内存。
	// "TieredReservation"：kubelet 按内存 request 为 Guaranteed Pod 设置 memory.min、为 Burstable Pod 设置 memory.low。
	// Guaranteed 内存不会被内核回收；Burstable 内存优先保留但极端压力下可能被回收。
	// 详见 https://kep.k8s.io/2570。
	// 默认值：None
	// +featureGate=MemoryQoS
	// +optional
	MemoryReservationPolicy MemoryReservationPolicy `json:"memoryReservationPolicy,omitempty"`
	// 只在节点首次注册时生效，后续修改配置不会更新已有污点。
	// registerWithTaints 是 kubelet 自注册时添加到 Node 对象的污点数组。
	// 仅在 registerNode 为 true 且节点首次注册时生效。
	// 默认值：nil
	// +optional
	RegisterWithTaints []v1.Taint `json:"registerWithTaints,omitempty"`
	// 关闭自动注册后需由外部流程创建 Node 对象。
	// registerNode 启用向 apiserver 自动注册。
	// 默认值：true
	// +optional
	RegisterNode *bool `json:"registerNode,omitempty"`
	// 复用 component-base 的 OpenTelemetry 配置，统一追踪导出行为。
	// Tracing 指定 OpenTelemetry tracing 客户端的版本化配置。
	// 详见 https://kep.k8s.io/2832。
	// 默认值：nil
	// +optional
	Tracing *tracingapi.TracingConfiguration `json:"tracing,omitempty"`

	// 依赖准确的根文件系统用量检测；kind rootless 等环境可能不支持而需关闭。
	// LocalStorageCapacityIsolation 启用本地临时存储隔离特性，默认开启。
	// 该特性允许用户为容器临时存储设置 request/limit，并像 cpu、memory 一样管理；
	// 也允许为 emptyDir 卷设置 sizeLimit，卷用量超限时触发 Pod 驱逐。
	// 卷用量超限时会触发 Pod 驱逐。
	// 该特性依赖正确检测根文件系统磁盘用量的能力。对某些系统（如 kind rootless），
	// 若不支持该能力应禁用此特性。禁用后用户不应为容器临时存储设置 request/limit 或为 emptyDir 设置 sizeLimit。
	// 禁用后不应为容器临时存储设置 request/limit 或为 emptyDir 设置 sizeLimit。
	// 默认值：true
	// +optional
	LocalStorageCapacityIsolation *bool `json:"localStorageCapacityIsolation,omitempty"`

	// Linux 常用 Unix socket，Windows 支持 npipe/tcp；必须与运行时实际监听地址一致。
	// ContainerRuntimeEndpoint 是容器运行时的端点。
	// Linux 上支持 Unix Domain Socket，Windows 上支持 npipe 与 tcp 端点。
	// 示例：'unix:///path/to/runtime.sock'、'npipe:////./pipe/runtime'
	ContainerRuntimeEndpoint string `json:"containerRuntimeEndpoint"`

	// 不设置时复用 containerRuntimeEndpoint，便于镜像服务单独部署。
	// ImageServiceEndpoint 是容器镜像服务的端点。
	// Linux 上支持 Unix Domain Socket，Windows 上支持 npipe 与 tcp 端点。
	// 示例：'unix:///path/to/runtime.sock'、'npipe:////./pipe/runtime'。
	// 未指定时使用 containerRuntimeEndpoint 的值。
	// +optional
	ImageServiceEndpoint string `json:"imageServiceEndpoint,omitempty"`

	// cgroup v1 已进入维护期；显式关闭此开关意味着接受旧内核带来的行为差异。
	// FailCgroupV1 阻止 kubelet 在使用 cgroup v1 的主机上启动。
	// 默认设为 'true'，即除非显式禁用，
	// kubelet 不会在 cgroup v1 主机上启动。
	// option is explicitly disabled.
	// 默认值：true
	// +optional
	FailCgroupV1 *bool `json:"failCgroupV1,omitempty"`

	// 用户命名空间相关配置，修改映射长度通常需要重建节点上的容器。
	// UserNamespaces 包含 User Namespace 的配置。
	// +optional
	UserNamespaces *UserNamespaces `json:"userNamespaces,omitempty"`
}

// KubeletAuthorizationMode 枚举 kubelet 服务请求的授权方式。
type KubeletAuthorizationMode string

const (
	// KubeletAuthorizationModeAlwaysAllow 放行所有已认证请求，只在完全信任调用方的环境使用。
	KubeletAuthorizationModeAlwaysAllow KubeletAuthorizationMode = "AlwaysAllow"
	// KubeletAuthorizationModeWebhook 借助 apiserver 的 SubjectAccessReview API 做细粒度授权，是生产环境的推荐选择。
	KubeletAuthorizationModeWebhook KubeletAuthorizationMode = "Webhook"
)

// KubeletAuthorization 把授权模式与 Webhook 参数打包成一个配置块。
type KubeletAuthorization struct {
	// mode 决定发往 kubelet 服务器的请求采用哪种授权模式，合法取值为 `AlwaysAllow` 与 `Webhook`，
	// 其中 Webhook 模式借助 SubjectAccessReview API 判定授权。授权模式决定已认证请求的后续判定路径。
	// +optional
	Mode KubeletAuthorizationMode `json:"mode,omitempty"`

	// webhook 是 Webhook 授权的相关设置，仅在 Mode 为 Webhook 时发挥作用。
	// +optional
	Webhook KubeletWebhookAuthorization `json:"webhook"`
}

// KubeletWebhookAuthorization 控制 Webhook 授权结果的缓存时长。
type KubeletWebhookAuthorization struct {
	// cacheAuthorizedTTL 是缓存 webhook 授权器 'authorized' 响应的时长。
	// 缓存放行结果可减少 SAR 调用，代价是权限撤销存在延迟。
	// +optional
	CacheAuthorizedTTL metav1.Duration `json:"cacheAuthorizedTTL,omitempty"`
	// cacheUnauthorizedTTL 是缓存 webhook 授权器 'unauthorized' 响应的时长。
	// 拒绝结果通常缓存更短，以便尽快反映权限变更。
	// +optional
	CacheUnauthorizedTTL metav1.Duration `json:"cacheUnauthorizedTTL,omitempty"`
}

// KubeletAuthentication 聚合 x509、Webhook、匿名三种认证方式的配置。
type KubeletAuthentication struct {
	// x509 是 x509 客户端证书认证的相关设置，适合机器对机器的固定身份调用。
	// +optional
	X509 KubeletX509Authentication `json:"x509"`
	// webhook 是 webhook bearer token 认证的相关设置，把 token 校验交给 apiserver 的 TokenReview API。
	// +optional
	Webhook KubeletWebhookAuthentication `json:"webhook"`
	// anonymous 是匿名认证的相关设置，其他认证方式未拒绝的请求才可能落到匿名通道。
	// +optional
	Anonymous KubeletAnonymousAuthentication `json:"anonymous"`
}

// KubeletX509Authentication 配置基于客户端证书的认证。
type KubeletX509Authentication struct {
	// clientCAFile 是 PEM 编码证书包的路径。设置后，凡出示由包中任一证书机构签发的
	// 客户端证书的请求都会被认证，证书中的 CommonName 与 Organization 会映射为用户名与用户组。
	// +optional
	ClientCAFile string `json:"clientCAFile,omitempty"`
}

// KubeletWebhookAuthentication 配置基于 TokenReview 的 bearer token 认证。
type KubeletWebhookAuthentication struct {
	// enabled 是否启用基于 tokenreviews.authentication.k8s.io API 的 bearer token 认证。
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// cacheTTL 开启认证结果缓存。缓存认证结果可减轻 apiserver 负担，但会延长 token 失效的感知时间。
	// +optional
	CacheTTL metav1.Duration `json:"cacheTTL,omitempty"`
}

// KubeletAnonymousAuthentication 控制是否允许匿名请求落到 kubelet 服务。
type KubeletAnonymousAuthentication struct {
	// enabled 允许匿名请求访问 kubelet 服务器。未被其他认证方式拒绝的请求会被视为匿名请求，
	// 其用户名为 `system:anonymous`，用户组名为 `system:unauthenticated`。
	// 默认拒绝匿名请求；确需放开时需评估节点上敏感端点的暴露风险。
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SerializedNodeConfigSource 允许序列化 v1.NodeConfigSource。
// kubelet 内部用它跟踪已做检查点的动态配置。
// 它放在 kubeletconfig API 组中，因为被视作 kubelet 的版本化输入。
type SerializedNodeConfigSource struct {
	metav1.TypeMeta `json:""`
	// source 是正在被序列化的配置来源。把配置来源对象整体序列化，
	// 便于 kubelet 重启后从检查点恢复。
	// +optional
	Source v1.NodeConfigSource `json:"source,omitempty" protobuf:"bytes,1,opt,name=source"`
}

// MemoryReservation 描述某个 NUMA 节点上各内存类型的预留量。
//
// NumaNode 指定节点编号，Limits 给出各资源类型在该节点上的预留量。
// 两个字段的对齐由 gofmt 决定，这里只说明含义。
type MemoryReservation struct {
	NumaNode int32           `json:"numaNode"`
	Limits   v1.ResourceList `json:"limits"`
}

// ShutdownGracePeriodByPodPriority 按 Pod 的优先级类值指定关闭宽限期。
type ShutdownGracePeriodByPodPriority struct {
	// priority 是与关机宽限期关联的优先级值，即优先级区间的下界，与列表中下一个条目共同确定范围。
	Priority int32 `json:"priority"`
	// shutdownGracePeriodSeconds 是关机宽限秒数，即该优先级区间对应的 Pod 关机宽限期。
	ShutdownGracePeriodSeconds int64 `json:"shutdownGracePeriodSeconds"`
}

// MemorySwapConfiguration 配置容器可用的 swap 行为，需 NodeSwap 门控配合。
type MemorySwapConfiguration struct {
	// swapBehavior 配置容器工作负载可用的 swap 内存，可取值为：
	// "" 或 "NoSwap"：工作负载不能使用 swap，为默认选项；
	// "LimitedSwap"：swap 用量受限，上限按容器的 memory request 比例计算。
	// +featureGate=NodeSwap
	// +optional
	SwapBehavior string `json:"swapBehavior,omitempty"`
}

// CrashLoopBackOffConfig 调整容器重启退避算法的上限，需特性门控开启。
type CrashLoopBackOffConfig struct {
	// maxContainerRestartPeriod 是容器重启退避延迟可累积的上限，最小 1 秒、最大 300 秒；
	// 不设置则沿用内部 crashloopbackoff 最大值（300s）。
	// +featureGate=KubeletCrashLoopBackOffMax
	// +optional
	MaxContainerRestartPeriod *metav1.Duration `json:"maxContainerRestartPeriod,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderConfig 包含各 exec 凭据插件的配置信息。
// kubelet 从磁盘读取该配置，
// 并按 CredentialProvider 类型启用每个 provider。
type CredentialProviderConfig struct {
	metav1.TypeMeta `json:""`

	// providers 是 kubelet 启用的凭据插件列表。多个插件可能匹配同一镜像，
	// 此时所有匹配插件的凭据都会返回给 kubelet 并合并；若插件返回重叠的凭据键，
	// 靠前的插件凭据优先尝试。
	Providers []CredentialProvider `json:"providers"`
}

// CredentialProvider 表示 kubelet 调用的 exec 插件。只有当被拉取镜像
// 匹配该插件处理的镜像时才会调用（见 matchImages）。
type CredentialProvider struct {
	// name 是凭据插件的必填名称，必须与 kubelet 可见的可执行文件名一致。
	// 可执行文件必须位于 kubelet 的 bin 目录（由 --image-credential-provider-bin-dir 标志设置），
	// 且名称必须在所有插件中唯一。
	Name string `json:"name"`

	// matchImages 是必填的字符串列表，用于匹配镜像以决定是否调用该插件。
	// 只要其中某个字符串匹配上 kubelet 请求的镜像，插件就会被调用并提供凭据。
	// 镜像需包含仓库域名与 URL 路径。
	//
	// matchImages 的每个条目都是模式，可选地包含端口与路径。通配符只能用于域名部分，
	// 不能用于端口或路径。支持 '*.k8s.io'、'k8s.*.io' 这类子域写法与 'k8s.*' 这类
	// 顶级域写法，也支持 'app*.k8s.io' 这样的部分子域匹配。每个通配符只匹配一段子域，
	// 因此 *.io 不会匹配 *.k8s.io。
	//
	// 镜像与 matchImage 满足以下全部条件才算匹配：
	// - 两者的域名段数相同且每段都匹配。
	// - matchImage 的 URL 路径必须是目标镜像 URL 路径的前缀。
	// - 若 matchImage 带有端口，镜像的端口必须与之相同。
	//
	// matchImages 取值示例：
	// - 123456789.dkr.ecr.us-east-1.amazonaws.com
	// - *.azurecr.io
	// - gcr.io
	// - *.*.registry.io
	// - registry.io:8080/path
	MatchImages []string `json:"matchImages"`

	// defaultCacheDuration 是必填字段：当插件响应未给出缓存时长时，插件在内存中缓存凭据所用时长的默认值。
	DefaultCacheDuration *metav1.Duration `json:"defaultCacheDuration"`

	// 调用 exec 插件时 CredentialProviderRequest 的必填输入版本。返回的 CredentialProviderResponse
	// 必须使用与请求相同的编码版本，否则插件调用失败。当前支持的取值：
	// - credentialprovider.kubelet.k8s.io/v1beta1
	APIVersion string `json:"apiVersion"`

	// 执行命令时传给插件可执行文件的命令行参数。
	// +optional
	Args []string `json:"args,omitempty"`

	// env 定义暴露给插件进程的附加环境变量，会与宿主环境以及 client-go
	// 用于向插件传参的变量合并。
	// +optional
	Env []ExecEnvVar `json:"env,omitempty"`
}

// ExecEnvVar 表示执行 exec 凭据插件进程时设置的单个环境变量。
//
// Name 是环境变量名，Value 是环境变量值。
type ExecEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// UserNamespaces 包含 User Namespace 的配置。
type UserNamespaces struct {
	// IDsPerPod 是 UID 与 GID 的映射长度，必须是 65536 的倍数且小于 1<<32；
	// 在 windows 等非 linux 系统上只允许为 null/缺省。修改该值可能需要重建节点上的所有容器。
	//
	// 默认值：65536
	// +optional
	IDsPerPod *int64 `json:"idsPerPod,omitempty"`
}

// ImagePullIntent 记录 kubelet 尝试拉取镜像的一次行为。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ImagePullIntent struct {
	metav1.TypeMeta `json:""`

	// Image 是容器 `image` 字段中的镜像 spec。文件名取该值的 SHA-256 哈希，
	// 以规避 ':'、'/' 等文件名不安全字符。
	Image string `json:"image"`
}

// ImagePullRecord 记录 kubelet 拉取过一个镜像。
//
// 若 `kubernetesSecrets` 字段无记录，且 `nodeWideCredentials` 与 `anonymous`
// 都为 `false`，下次请求该记录对应的镜像时必须重新校验凭据。
// 下次请求该记录对应的镜像时必须重新校验凭据。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ImagePulledRecord struct {
	metav1.TypeMeta `json:""`

	// LastUpdatedTime 是本记录最近一次更新的时间，用于判断记录是否仍然可信。
	LastUpdatedTime metav1.Time `json:"lastUpdatedTime"`

	// ImageRef 是来自 CRI 的本记录所代表镜像的引用。文件名同样取该值的 SHA-256 哈希，
	// 以规避 ':'、'/' 等文件名不安全字符。
	ImageRef string `json:"imageRef"`

	// CredentialMapping 把 `image` 映射到之前拉取它时使用的凭据集合。
	// previously pulled with.
	// 这里的 `image` 是 Pod 容器 `image` 字段去掉 tag/digest 后的内容。
	// 是 Pod 容器 `image` 字段去掉 tag/digest 后的内容。
	//
	// 示例：
	// 容器请求 `hello-world:latest@sha256:91fb4b041da273d5a3273b6d587d62d518300a6ad268b28628f74997b93171b2` 镜像：
	// "credentialMapping": {
	// "hello-world": { "nodePodsAccessible": true }
	//     }
	// 键是去掉 tag/digest 的镜像名，值为该镜像此前拉取时使用的凭据集合。
	CredentialMapping map[string]ImagePullCredentials `json:"credentialMapping,omitempty"`
}

// ImagePullCredentials 记录某个镜像拉取时可使用/使用过的凭据来源，供后续校验复用。
type ImagePullCredentials struct {
	// KubernetesSecretCoordinates 是拉取该镜像时用到的所有 Kubernetes Secret 的坐标索引，
	// 便于镜像复用时快速核对凭据是否仍有效。
	// +optional
	// +listType=set
	KubernetesSecrets []ImagePullSecret `json:"kubernetesSecrets,omitempty"`

	// KubernetesServiceAccounts 是拉取该镜像时用到的所有 Kubernetes ServiceAccount 的坐标索引，
	// 即曾向凭据插件提供 token 的 ServiceAccount。
	// +optional
	// +listType=set
	KubernetesServiceAccounts []ImagePullServiceAccount `json:"kubernetesServiceAccounts,omitempty"`

	// NodePodsAccessible 表示拉取凭据可被节点上所有 Pod 使用，或该次拉取无需凭据。
	//
	// 为 true 时与 `kubernetesSecrets` 字段互斥。
	// +optional
	NodePodsAccessible bool `json:"nodePodsAccessible,omitempty"`
}

// ImagePullSecret 用 Kubernetes Secret 对象坐标加上其拉取凭据内容的凭据哈希，
// 索引一次拉取所用的 Secret。
type ImagePullSecret struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`

	// CredentialHash 是对 UID/Namespace/Name 坐标所指定 Secret 中的镜像拉取凭据内容
	// 做 SHA-256 得到的哈希。保存哈希而非原文，既能比对变化又避免落盘敏感信息。
	CredentialHash string `json:"credentialHash"`
}

// ImagePullServiceAccount 记录一个 Kubernetes ServiceAccount 对象的坐标：
// kubelet 曾为获取镜像拉取凭据，把该 ServiceAccount 的 token 发送给凭据插件。
type ImagePullServiceAccount struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
