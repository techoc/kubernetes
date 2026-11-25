/*
Copyright The Kubernetes Authors.

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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InitConfiguration 包含仅适用于 `kubeadm init` 运行时信息的字段。
type InitConfiguration struct {
	metav1.TypeMeta `json:""`

	// 以下字段仅供首次运行 `kubeadm init` 时使用，之后不会上传到 `kubeadm-config` ConfigMap，
	// 也不会供 `kubeadm upgrade` 等命令使用。这些字段必须设置为 omitempty。

	// BootstrapTokens 描述 `kubeadm init` 时要创建的一组 Bootstrap Token。
	// 这些信息包含敏感内容，不会上传到集群的 ConfigMap。
	// +optional
	BootstrapTokens []bootstraptokenv1.BootstrapToken `json:"bootstrapTokens,omitempty"`

	// DryRun 指示是否启用演练模式；启用时不应用任何更改，仅输出将要执行的操作。
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// NodeRegistration 保存将新控制平面节点注册到集群所需的字段。
	// +optional
	NodeRegistration NodeRegistrationOptions `json:"nodeRegistration,omitempty"`

	// LocalAPIEndpoint 表示部署在此控制平面节点上的 API Server 实例的端点。
	// 在高可用集群中，它不同于 ClusterConfiguration.ControlPlaneEndpoint：后者是集群的全局端点，
	// 负责将请求负载均衡到各个 API Server。此字段用于自定义本地 API Server 公布的 IP/DNS 名称和端口。
	// 默认情况下，kubeadm 会尝试自动探测默认网卡的 IP；探测失败时可在此显式设置。
	// +optional
	LocalAPIEndpoint APIEndpoint `json:"localAPIEndpoint,omitempty"`

	// CertificateKey 设置证书密钥，用于在 uploadcerts 初始化阶段将证书和私钥加密后上传到集群 Secret。
	// 证书密钥是长度为 32 字节的 AES 密钥的十六进制编码字符串。
	// +optional
	CertificateKey string `json:"certificateKey,omitempty"`

	// SkipPhases 是命令执行时要跳过的阶段列表，可通过 "kubeadm init --help" 查看。
	// 命令行参数 "--skip-phases" 的优先级高于此字段。
	// +optional
	SkipPhases []string `json:"skipPhases,omitempty"`

	// Patches 保存 `kubeadm init` 期间向 kubeadm 部署的组件应用补丁所需的选项。
	// +optional
	Patches *Patches `json:"patches,omitempty"`

	// Timeouts 保存适用于 kubeadm 命令的各种超时设置。
	// +optional
	Timeouts *Timeouts `json:"timeouts,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterConfiguration 包含 kubeadm 集群的全局配置。
type ClusterConfiguration struct {
	metav1.TypeMeta `json:""`

	// Etcd 保存 etcd 配置。
	// +optional
	Etcd Etcd `json:"etcd,omitempty"`

	// Networking 保存集群的网络拓扑配置。
	// +optional
	Networking Networking `json:"networking,omitempty"`

	// KubernetesVersion 是控制平面的目标版本。
	// +optional
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// ControlPlaneEndpoint 为控制平面设置稳定的 IP 地址或 DNS 名称；该值可以是有效 IP 地址，
	// 也可以是符合 RFC-1123 的 DNS 子域名，两者都可附带 TCP 端口。
	// 未指定时使用 AdvertiseAddress + BindPort；已指定但未包含 TCP 端口时使用 BindPort。
	// 例如，多控制平面集群应将其设置为控制平面实例前端外部负载均衡器的地址；
	// 在频繁回收节点的环境中，可用它为控制平面分配稳定的 DNS 名称。
	// +optional
	ControlPlaneEndpoint string `json:"controlPlaneEndpoint,omitempty"`

	// APIServer 保存 API Server 控制平面组件的额外设置。
	// +optional
	APIServer APIServer `json:"apiServer,omitempty"`

	// ControllerManager 保存控制器管理器控制平面组件的额外设置。
	// +optional
	ControllerManager ControlPlaneComponent `json:"controllerManager,omitempty"`

	// Scheduler 保存调度器控制平面组件的额外设置。
	// +optional
	Scheduler ControlPlaneComponent `json:"scheduler,omitempty"`

	// DNS 定义集群中安装的 DNS 插件的选项。
	// +optional
	DNS DNS `json:"dns,omitempty"`

	// Proxy 定义集群中安装的代理插件的选项。
	Proxy Proxy `json:"proxy,omitempty"`

	// CertificatesDir 指定存放或查找所有必需证书的目录。
	// +optional
	CertificatesDir string `json:"certificatesDir,omitempty"`

	// ImageRepository 设置拉取容器镜像所用的镜像仓库。
	// 为空时默认使用 `registry.k8s.io`；若 Kubernetes 版本为 CI 构建（版本以 `ci/` 开头），
	// 控制平面组件和 kube-proxy 默认使用 `gcr.io/k8s-staging-ci-images`，其他镜像仍使用 `registry.k8s.io`。
	// +optional
	ImageRepository string `json:"imageRepository,omitempty"`

	// FeatureGates 保存用户启用的 kubeadm 专属特性门控。
	// +optional
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// ClusterName 是集群名称，用于 kubeadm 生成的 kubeconfig 文件，
	// 也会作为 kube-controller-manager 的 --cluster-name 参数值。默认值为 'kubernetes'。
	// +optional
	ClusterName string `json:"clusterName,omitempty"`

	// EncryptionAlgorithm 保存生成密钥和证书所用的非对称加密算法类型。
	// 可选值为 "RSA-2048"（默认）、"RSA-3072"、"RSA-4096"、"ECDSA-P256" 或 "ECDSA-P384"。
	// +optional
	EncryptionAlgorithm EncryptionAlgorithmType `json:"encryptionAlgorithm,omitempty"`

	// CertificateValidityPeriod 指定 kubeadm 生成的非 CA 证书的有效期。
	// 默认值：8760h（365 天 × 24 小时 = 1 年）。
	// +optional
	CertificateValidityPeriod *metav1.Duration `json:"certificateValidityPeriod,omitempty"`

	// CACertificateValidityPeriod 指定 kubeadm 生成的 CA 证书的有效期。
	// 默认值：87600h（365 天 × 24 小时 × 10 = 10 年）。
	// +optional
	CACertificateValidityPeriod *metav1.Duration `json:"caCertificateValidityPeriod,omitempty"`
}

// ControlPlaneComponent 保存集群控制平面组件的通用设置。
type ControlPlaneComponent struct {
	// ExtraArgs 是传递给控制平面组件的额外参数。
	// 参数名为命令行中的参数名去掉前导短横线；额外参数会覆盖同名默认参数，允许重复。
	// 默认参数按字母数字顺序排序，额外参数不排序。
	// +optional
	ExtraArgs []Arg `json:"extraArgs,omitempty"`

	// ExtraVolumes 是挂载到控制平面组件的一组额外宿主主机卷。
	// +optional
	ExtraVolumes []HostPathMount `json:"extraVolumes,omitempty"`

	// ExtraEnvs 是传递给控制平面组件的一组额外环境变量。
	// 其中设置的环境变量会覆盖已有环境变量，以及 kubeadm 默认添加的 *_proxy 环境变量。
	// +optional
	ExtraEnvs []EnvVar `json:"extraEnvs,omitempty"`
}

// APIServer 保存集群中部署 API Server 所需的设置。
type APIServer struct {
	ControlPlaneComponent `json:""`

	// CertSANs 为 API Server 签名证书设置额外的 Subject Alternative Name。
	// +optional
	CertSANs []string `json:"certSANs,omitempty"`
}

// DNS 定义集群中使用的 DNS 插件。
type DNS struct {
	// ImageMeta 允许自定义 DNS 插件使用的镜像。
	ImageMeta `json:""`

	// Disabled 指定是否在集群中禁用此插件。
	// +optional
	Disabled bool `json:"disabled,omitempty"`
}

// Proxy 定义集群中使用的代理插件。
type Proxy struct {
	// Disabled 指定是否在集群中禁用此插件。
	// +optional
	Disabled bool `json:"disabled,omitempty"`
}

// ImageMeta 允许自定义不来自 Kubernetes/Kubernetes 发布流程的组件镜像。
type ImageMeta struct {
	// ImageRepository 设置拉取容器镜像所用的镜像仓库。
	// 未设置时使用 ClusterConfiguration 中定义的 ImageRepository。
	// +optional
	ImageRepository string `json:"imageRepository,omitempty"`

	// ImageTag 允许指定镜像标签。
	// 设置此值后，kubeadm 在升级期间不会自动更改上述组件的版本。
	// +optional
	ImageTag string `json:"imageTag,omitempty"`

	// TODO: 根据用户反馈评估是否还需要 ImageName。
}

// APIEndpoint 包含部署在节点上的 API Server 实例的端点信息。
type APIEndpoint struct {
	// AdvertiseAddress 设置 API Server 公布的 IP 地址。
	// +optional
	AdvertiseAddress string `json:"advertiseAddress,omitempty"`

	// BindPort 设置 API Server 监听的安全端口，默认为 6443。
	// +optional
	BindPort int32 `json:"bindPort,omitempty"`
}

// NodeRegistrationOptions 保存通过 `kubeadm init` 或 `kubeadm join` 将新控制平面节点或工作节点注册到集群所需的字段。
type NodeRegistrationOptions struct {

	// Name 是本次 `kubeadm init` 或 `kubeadm join` 操作创建的 Node API 对象的 `.Metadata.Name` 字段值。
	// 此字段也用于 kubelet 连接 API Server 的客户端证书的 CommonName。未提供时默认为节点主机名。
	// +optional
	Name string `json:"name,omitempty"`

	// CRISocket 用于获取容器运行时信息，这些信息会标注到 Node API 对象上供后续复用。
	// +optional
	CRISocket string `json:"criSocket,omitempty"`

	// Taints 指定注册 Node API 对象时使用的污点。未设置（即 nil）时，控制平面节点默认添加控制平面污点。
	// 若不希望为控制平面节点添加污点，请在 YAML 文件中将此字段设置为空切片，即 `taints: []`。
	// 此字段仅用于节点注册。
	Taints []corev1.Taint `json:"taints"`

	// KubeletExtraArgs 是传递给 kubelet 的额外参数。kubeadm 在运行时写入供 kubelet 加载的环境文件，
	// 再通过这些文件将参数传入 kubelet 命令行。解析时这些参数的优先级高于 kubelet-config ConfigMap 中的通用基础配置。
	// 这些值仅对运行 kubeadm 的节点生效。参数名为命令行参数名去掉前导短横线；额外参数会覆盖同名默认参数，允许重复。
	// 默认参数按字母数字顺序排序，额外参数不排序。
	// +optional
	KubeletExtraArgs []Arg `json:"kubeletExtraArgs,omitempty"`

	// IgnorePreflightErrors 是注册当前节点时要忽略的预检错误列表，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	// +optional
	IgnorePreflightErrors []string `json:"ignorePreflightErrors,omitempty"`

	// ImagePullPolicy 指定 kubeadm 执行 init 和 join 时的镜像拉取策略。
	// 取值必须为 "Always"、"IfNotPresent" 或 "Never"；未设置时默认为 "IfNotPresent"，即仅在所需镜像不存在时拉取。
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSerial 指定 kubeadm 应串行还是并行拉取镜像，默认为 true。
	// +optional
	ImagePullSerial *bool `json:"imagePullSerial,omitempty"`
}

// Networking 包含集群网络配置。
type Networking struct {
	// ServiceSubnet 是 Kubernetes 服务使用的子网，默认为 "10.96.0.0/12"。
	// +optional
	ServiceSubnet string `json:"serviceSubnet,omitempty"`
	// PodSubnet 是 Pod 使用的子网。
	// +optional
	PodSubnet string `json:"podSubnet,omitempty"`
	// DNSDomain 是 Kubernetes 服务使用的 DNS 域，默认为 "cluster.local"。
	// +optional
	DNSDomain string `json:"dnsDomain,omitempty"`
}

// Etcd 包含 etcd 配置。
type Etcd struct {

	// Local 保存本地 etcd 实例的配置。Local 与 External 互斥。
	// +optional
	Local *LocalEtcd `json:"local,omitempty"`

	// External 描述如何连接外部 etcd 集群。Local 与 External 互斥。
	// +optional
	External *ExternalEtcd `json:"external,omitempty"`
}

// LocalEtcd 表示 kubeadm 应在本地运行 etcd 集群。
type LocalEtcd struct {
	// ImageMeta 允许自定义 etcd 容器镜像。指定自定义 etcd 镜像后，kubeadm upgrade 会将其视为用户管理的镜像并跳过升级。
	ImageMeta `json:""`

	// DataDir 是 etcd 存放数据的目录，默认为 "/var/lib/etcd"。
	DataDir string `json:"dataDir"`

	// ExtraArgs 是 etcd 在静态 Pod 中运行时接收的额外参数。
	// 参数名为命令行参数名去掉前导短横线；额外参数会覆盖同名默认参数，允许重复。
	// 默认参数按字母数字顺序排序，额外参数不排序。
	// +optional
	ExtraArgs []Arg `json:"extraArgs,omitempty"`

	// ExtraEnvs 是传递给控制平面组件的一组额外环境变量。
	// 其中设置的环境变量会覆盖已有环境变量，以及 kubeadm 默认添加的 *_proxy 环境变量。
	// +optional
	ExtraEnvs []EnvVar `json:"extraEnvs,omitempty"`

	// ServerCertSANs 为 etcd 服务端签名证书设置额外的 Subject Alternative Name。
	// +optional
	ServerCertSANs []string `json:"serverCertSANs,omitempty"`
	// PeerCertSANs 为 etcd 对端签名证书设置额外的 Subject Alternative Name。
	// +optional
	PeerCertSANs []string `json:"peerCertSANs,omitempty"`
}

// ExternalEtcd 描述外部 etcd 集群。kubeadm 无法获知证书文件的位置，因此必须由用户提供。
type ExternalEtcd struct {
	// Endpoints 是 etcd 成员端点，使用外部 etcd 时必填。
	// 这些客户端 URL（通常为 gRPC 端点）默认同时处理 gRPC 流量（etcd 主要协议）和 HTTP 流量（指标、健康检查）。
	// 配置 HTTPEndpoints 后，两类流量可分离以提升安全性和性能。对应 etcd 的 --listen-client-urls 配置。
	Endpoints []string `json:"endpoints"`

	// HTTPEndpoints 是 etcd 通信专用的 HTTP 端点。
	// 配置后，/metrics 和 /health 等 HTTP 流量会与 Endpoints 处理的 gRPC 流量分离，
	// 从而允许只暴露 HTTP 端点而不暴露主要 gRPC 接口，便于访问控制。
	// 对应 etcd 的 --listen-client-http-urls 配置；未提供时，两类流量均使用 Endpoints。
	// +optional
	HTTPEndpoints []string `json:"httpEndpoints,omitempty"`

	// CAFile 是用于保护 etcd 通信的 SSL 证书颁发机构文件。使用 TLS 连接时必填。
	CAFile string `json:"caFile"`

	// CertFile 是用于保护 etcd 通信的 SSL 证书文件。使用 TLS 连接时必填。
	CertFile string `json:"certFile"`

	// KeyFile 是用于保护 etcd 通信的 SSL 私钥文件。使用 TLS 连接时必填。
	KeyFile string `json:"keyFile"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// JoinConfiguration 包含加入集群的节点相关配置。
type JoinConfiguration struct {
	metav1.TypeMeta `json:""`

	// DryRun 指示是否启用演练模式；启用时不应用任何更改，仅输出将要执行的操作。
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// NodeRegistration 保存将新控制平面节点注册到集群所需的字段。
	// +optional
	NodeRegistration NodeRegistrationOptions `json:"nodeRegistration,omitempty"`

	// CACertPath 是用于保护节点与控制平面通信的 SSL 证书颁发机构文件路径。
	// 默认为 "/etc/kubernetes/pki/ca.crt"。
	// +optional
	CACertPath string `json:"caCertPath,omitempty"`

	// Discovery 指定 kubelet 在 TLS Bootstrap 过程中使用的选项。
	Discovery Discovery `json:"discovery"`

	// ControlPlane 定义要部署在加入集群节点上的额外控制平面实例。为 nil 时不部署额外控制平面实例。
	// +optional
	ControlPlane *JoinControlPlane `json:"controlPlane,omitempty"`

	// SkipPhases 是命令执行时要跳过的阶段列表，可通过 "kubeadm join --help" 查看。
	// 命令行参数 "--skip-phases" 的优先级高于此字段。
	// +optional
	SkipPhases []string `json:"skipPhases,omitempty"`

	// Patches 保存 `kubeadm join` 期间向 kubeadm 部署的组件应用补丁所需的选项。
	// +optional
	Patches *Patches `json:"patches,omitempty"`

	// Timeouts 保存适用于 kubeadm 命令的各种超时设置。
	// +optional
	Timeouts *Timeouts `json:"timeouts,omitempty"`
}

// JoinControlPlane 包含部署在加入集群节点上的额外控制平面实例相关配置。
type JoinControlPlane struct {
	// LocalAPIEndpoint 表示将部署在此节点上的 API Server 实例的端点。
	// +optional
	LocalAPIEndpoint APIEndpoint `json:"localAPIEndpoint,omitempty"`

	// CertificateKey 用于新控制平面节点加入时解密从 Secret 下载的证书，对应的加密密钥位于 InitConfiguration 中。
	// 证书密钥是长度为 32 字节的 AES 密钥的十六进制编码字符串。
	// +optional
	CertificateKey string `json:"certificateKey,omitempty"`
}

// Discovery 指定 kubelet 在 TLS Bootstrap 过程中使用的选项。
type Discovery struct {
	// BootstrapToken 设置基于 Bootstrap Token 的发现选项。BootstrapToken 与 File 互斥。
	// +optional
	BootstrapToken *BootstrapTokenDiscovery `json:"bootstrapToken,omitempty"`

	// File 指定用于加载集群信息的 kubeconfig 文件或 URL。BootstrapToken 与 File 互斥。
	// +optional
	File *FileDiscovery `json:"file,omitempty"`

	// TLSBootstrapToken 是用于 TLS 引导的令牌。
	// 设置 .BootstrapToken 时，此字段默认使用 .BootstrapToken.Token，但可覆盖。
	// 设置 .File 时，若 kubeconfig 文件不包含其他认证信息，则必须设置此字段。
	// +optional
	TLSBootstrapToken string `json:"tlsBootstrapToken,omitempty" datapolicy:"token"`
}

// BootstrapTokenDiscovery 设置基于 Bootstrap Token 的发现选项。
type BootstrapTokenDiscovery struct {
	// Token 是用于验证从控制平面获取的集群信息的令牌。
	Token string `json:"token" datapolicy:"token"`

	// APIServerEndpoint 是要从中获取信息的 API Server 的 IP 地址或域名。
	// +optional
	APIServerEndpoint string `json:"apiServerEndpoint,omitempty"`

	// CACertHashes 指定基于令牌发现时用于验证的一组公钥固定值，发现得到的根 CA 必须匹配其中之一。
	// 设置为空集合会禁用根 CA 固定，可能不安全。每个哈希值采用 "<type>:<value>" 格式，
	// 目前唯一支持的类型为 "sha256"。哈希值是对 DER 编码的 ASN.1 中 Subject Public Key Info (SPKI)
	// 对象计算的 SHA-256 十六进制摘要，可使用 OpenSSL 等工具计算。
	// +optional
	CACertHashes []string `json:"caCertHashes,omitempty" datapolicy:"security-key"`

	// UnsafeSkipCAVerification 允许基于令牌的发现跳过通过 CACertHashes 进行的 CA 验证。
	// 其他节点可能因此冒充控制平面，削弱 kubeadm 的安全性。
	// +optional
	UnsafeSkipCAVerification bool `json:"unsafeSkipCAVerification,omitempty"`
}

// FileDiscovery 指定用于加载集群信息的 kubeconfig 文件或 URL。
type FileDiscovery struct {
	// KubeConfigPath 指定用于加载集群信息的 kubeconfig 文件的实际路径或 URL。
	KubeConfigPath string `json:"kubeConfigPath"`
}

// HostPathMount 包含从宿主机挂载的卷相关信息。
type HostPathMount struct {
	// Name 是 Pod 模板中的卷名称。
	Name string `json:"name"`
	// HostPath 是将挂载到 Pod 内的宿主机路径。
	HostPath string `json:"hostPath"`
	// MountPath 是 Pod 内挂载 HostPath 的路径。
	MountPath string `json:"mountPath"`
	// ReadOnly 控制卷是否只读。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty"`
	// PathType 是 HostPath 的类型。
	// +optional
	PathType corev1.HostPathType `json:"pathType,omitempty"`
}

// Patches 包含向 kubeadm 部署的组件应用补丁所需的选项。
type Patches struct {
	// Directory 是存放补丁文件的目录，文件名格式为 "target[suffix][+patchtype].extension"。
	// 例如 "kube-apiserver0+merge.yaml" 或 "etcd.json"。"target" 可选 "kube-apiserver"、
	// "kube-controller-manager"、"kube-scheduler"、"etcd"、"kubeletconfiguration"、"corednsdeployment" 或 "kubeproxydaemonset"。
	// "patchtype" 可选 "strategic"、"merge" 或 "json"，对应 kubectl 支持的补丁格式；默认值为 "strategic"。
	// "extension" 必须为 "json" 或 "yaml"。可选 "suffix" 用于按字母数字顺序确定补丁的应用先后。
	// +optional
	Directory string `json:"directory,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResetConfiguration 包含仅适用于 `kubeadm reset` 运行时信息的字段。
type ResetConfiguration struct {
	metav1.TypeMeta `json:""`

	// CleanupTmpDir 指定重置过程中是否清理 "/etc/kubernetes/tmp" 目录。
	// +optional
	CleanupTmpDir bool `json:"cleanupTmpDir,omitempty"`

	// CertificatesDir 指定证书存放目录。指定后，重置过程中会清理该目录。
	// +optional
	CertificatesDir string `json:"certificatesDir,omitempty"`

	// CRISocket 用于获取容器运行时信息并删除容器。
	// 若命令行参数和配置文件均未指定 CRISocket，kubeadm 会尝试探测一个有效值。
	// +optional
	CRISocket string `json:"criSocket,omitempty"`

	// DryRun 指示是否启用演练模式；启用时不应用任何更改，仅输出将要执行的操作。
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// Force 指示 kubeadm 重置节点时无需确认。
	// +optional
	Force bool `json:"force,omitempty"`

	// IgnorePreflightErrors 是重置过程中要忽略的预检错误列表，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	// +optional
	IgnorePreflightErrors []string `json:"ignorePreflightErrors,omitempty"`

	// SkipPhases 是命令执行时要跳过的阶段列表，可通过 "kubeadm reset phase --help" 查看。
	// +optional
	SkipPhases []string `json:"skipPhases,omitempty"`

	// UnmountFlags 是 kubeadm 在 reset 期间卸载目录时可使用的 unmount2() 系统调用参数列表。
	// 可选值为 MNT_FORCE、MNT_DETACH、MNT_EXPIRE、UMOUNT_NOFOLLOW，默认为空列表。
	// +optional
	UnmountFlags []string `json:"unmountFlags,omitempty"`

	// Timeouts 保存适用于 kubeadm 命令的各种超时设置。
	// +optional
	Timeouts *Timeouts `json:"timeouts,omitempty"`
}

// ArgMergeMethod 定义同名参数的合并方式。
type ArgMergeMethod string

const (
	// ArgMergeMethodAppend 将新值追加到现有参数值末尾。
	ArgMergeMethodAppend ArgMergeMethod = "append"
	// ArgMergeMethodPrepend 将新值插入现有参数值开头。
	ArgMergeMethodPrepend ArgMergeMethod = "prepend"
)

// Arg 表示带名称和值的参数。
// MergeMethod 定义此参数与同名基础参数的合并方式；未指定时默认替换基础参数。
// 若多个同名覆盖参数都定义了合并方式，则使用最后一个。
// 若覆盖参数定义了合并方式但没有匹配的基础参数，则忽略该覆盖参数。
type Arg struct {
	// Name 是参数名，匹配时按完整名称区分大小写。
	Name string `json:"name"`
	// Value 是参数值；是否允许独立值由使用方决定。
	Value string `json:"value"`
	// MergeMethod 指定与同名基础参数的合并方式；未设置时替换基础参数。
	MergeMethod ArgMergeMethod `json:"mergeMethod,omitempty"`
}

// EnvVar 表示容器中的环境变量。
type EnvVar struct {
	corev1.EnvVar `json:""`
}

// EncryptionAlgorithmType 表示非对称加密算法类型。
type EncryptionAlgorithmType string

const (
	// EncryptionAlgorithmECDSAP256 表示使用 P-256 曲线的 ECDSA 加密算法。
	EncryptionAlgorithmECDSAP256 EncryptionAlgorithmType = "ECDSA-P256"
	// EncryptionAlgorithmECDSAP384 表示使用 P-384 曲线的 ECDSA 加密算法。
	EncryptionAlgorithmECDSAP384 EncryptionAlgorithmType = "ECDSA-P384"
	// EncryptionAlgorithmRSA2048 表示密钥长度为 2048 位的 RSA 加密算法。
	EncryptionAlgorithmRSA2048 EncryptionAlgorithmType = "RSA-2048"
	// EncryptionAlgorithmRSA3072 表示密钥长度为 3072 位的 RSA 加密算法。
	EncryptionAlgorithmRSA3072 EncryptionAlgorithmType = "RSA-3072"
	// EncryptionAlgorithmRSA4096 表示密钥长度为 4096 位的 RSA 加密算法。
	EncryptionAlgorithmRSA4096 EncryptionAlgorithmType = "RSA-4096"
)

// Timeouts 保存适用于 kubeadm 命令的各种超时设置。
type Timeouts struct {
	// ControlPlaneComponentHealthCheck 是 `kubeadm init` 和 `kubeadm join` 期间等待 API Server 等控制平面组件健康的超时时间，默认为 4m。
	// +optional
	ControlPlaneComponentHealthCheck *metav1.Duration `json:"controlPlaneComponentHealthCheck,omitempty"`

	// KubeletHealthCheck 是 `kubeadm init` 和 `kubeadm join` 期间等待 kubelet 健康的超时时间，默认为 4m。
	// +optional
	KubeletHealthCheck *metav1.Duration `json:"kubeletHealthCheck,omitempty"`

	// KubernetesAPICall 是等待 kubeadm 客户端完成 API Server 请求的超时时间，适用于 GET、POST 等所有方法，默认为 1m。
	// +optional
	KubernetesAPICall *metav1.Duration `json:"kubernetesAPICall,omitempty"`

	// EtcdAPICall 是等待 kubeadm 的 etcd 客户端完成集群请求的超时时间，默认为 2m。
	// +optional
	EtcdAPICall *metav1.Duration `json:"etcdAPICall,omitempty"`

	// TLSBootstrap 是等待加入集群节点的 kubelet 完成 TLS 引导的超时时间，默认为 5m。
	// +optional
	TLSBootstrap *metav1.Duration `json:"tlsBootstrap,omitempty"`

	// Discovery 是 kubeadm 为加入集群节点验证 API Server 身份的超时时间，默认为 5m。
	// +optional
	Discovery *metav1.Duration `json:"discovery,omitempty"`

	// UpgradeManifests 是升级静态 Pod 清单的超时时间，默认为 5m。
	UpgradeManifests *metav1.Duration `json:"upgradeManifests,omitempty"`
}

// UpgradeApplyConfiguration 包含专用于 `kubeadm upgrade apply` 命令的可配置选项。
type UpgradeApplyConfiguration struct {
	// KubernetesVersion 是控制平面的目标版本。
	// +optional
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// AllowExperimentalUpgrades 指示 kubeadm 将不稳定的 Kubernetes 版本列为升级候选，并允许升级到 alpha、beta 或候选版本，默认为 false。
	// +optional
	AllowExperimentalUpgrades *bool `json:"allowExperimentalUpgrades,omitempty"`

	// AllowRCUpgrades 指示 kubeadm 将 Kubernetes 候选版本列为升级候选，并允许升级到候选版本。
	// +optional
	AllowRCUpgrades *bool `json:"allowRCUpgrades,omitempty"`

	// CertificateRenewal 指示 kubeadm 在升级期间执行证书续期，默认为 true。
	// +optional
	CertificateRenewal *bool `json:"certificateRenewal,omitempty"`

	// DryRun 指示是否启用演练模式；启用时不应用任何更改，仅输出将要执行的操作。
	// +optional
	DryRun *bool `json:"dryRun,omitempty"`

	// EtcdUpgrade 指示 kubeadm 在升级期间执行 etcd 升级，默认为 true。
	// +optional
	EtcdUpgrade *bool `json:"etcdUpgrade,omitempty"`

	// ForceUpgrade 指示 kubeadm 升级集群时无需确认。
	// +optional
	ForceUpgrade *bool `json:"forceUpgrade,omitempty"`

	// IgnorePreflightErrors 是升级过程中要忽略的预检错误列表，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	// +optional
	IgnorePreflightErrors []string `json:"ignorePreflightErrors,omitempty"`

	// Patches 保存 `kubeadm upgrade` 期间向 kubeadm 部署的组件应用补丁所需的选项。
	// +optional
	Patches *Patches `json:"patches,omitempty"`

	// PrintConfig 指定是否打印升级将使用的配置文件。
	// +optional
	PrintConfig *bool `json:"printConfig,omitempty"`

	// SkipPhases 是命令执行时要跳过的阶段列表。
	// 注意：`kubeadm upgrade apply` 当前忽略此字段，未来将支持。
	SkipPhases []string `json:"skipPhases,omitempty"`

	// ImagePullPolicy 指定 kubeadm 执行 upgrade apply 时的镜像拉取策略。
	// 取值必须为 "Always"、"IfNotPresent" 或 "Never"；未设置时默认为 "IfNotPresent"，即仅在所需镜像不存在时拉取。
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSerial 指定 kubeadm 应串行还是并行拉取镜像，默认为 true。
	// +optional
	ImagePullSerial *bool `json:"imagePullSerial,omitempty"`
}

// UpgradeDiffConfiguration 包含专用于 `kubeadm upgrade diff` 命令的可配置选项。
type UpgradeDiffConfiguration struct {
	// KubernetesVersion 是控制平面的目标版本。
	// +optional
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// DiffContextLines 是差异输出中的上下文行数。
	// +optional
	DiffContextLines int `json:"contextLines,omitempty"`
}

// UpgradeNodeConfiguration 包含专用于 `kubeadm upgrade node` 命令的可配置选项。
type UpgradeNodeConfiguration struct {
	// CertificateRenewal 指示 kubeadm 在升级期间执行证书续期，默认为 true。
	// +optional
	CertificateRenewal *bool `json:"certificateRenewal,omitempty"`

	// DryRun 指示是否启用演练模式；启用时不应用任何更改，仅输出将要执行的操作。
	// +optional
	DryRun *bool `json:"dryRun,omitempty"`

	// EtcdUpgrade 指示 kubeadm 在升级期间执行 etcd 升级，默认为 true。
	// +optional
	EtcdUpgrade *bool `json:"etcdUpgrade,omitempty"`

	// IgnorePreflightErrors 是升级过程中要忽略的预检错误列表，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	// +optional
	IgnorePreflightErrors []string `json:"ignorePreflightErrors,omitempty"`

	// SkipPhases 是命令执行时要跳过的阶段列表，可通过 "kubeadm upgrade node phase --help" 查看。
	// +optional
	SkipPhases []string `json:"skipPhases,omitempty"`

	// Patches 保存 `kubeadm upgrade` 期间向 kubeadm 部署的组件应用补丁所需的选项。
	// +optional
	Patches *Patches `json:"patches,omitempty"`

	// ImagePullPolicy 指定 kubeadm 执行 upgrade node 时的镜像拉取策略。
	// 取值必须为 "Always"、"IfNotPresent" 或 "Never"；未设置时默认为 "IfNotPresent"，即仅在所需镜像不存在时拉取。
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSerial 指定 kubeadm 应串行还是并行拉取镜像，默认为 true。
	// +optional
	ImagePullSerial *bool `json:"imagePullSerial,omitempty"`
}

// UpgradePlanConfiguration 包含专用于 `kubeadm upgrade plan` 命令的可配置选项。
type UpgradePlanConfiguration struct {
	// KubernetesVersion 是控制平面的目标版本。
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// AllowExperimentalUpgrades 指示 kubeadm 将不稳定的 Kubernetes 版本列为升级候选，并允许升级到 alpha、beta 或候选版本，默认为 false。
	// +optional
	AllowExperimentalUpgrades *bool `json:"allowExperimentalUpgrades,omitempty"`

	// AllowRCUpgrades 指示 kubeadm 将 Kubernetes 候选版本列为升级候选，并允许升级到候选版本。
	// +optional
	AllowRCUpgrades *bool `json:"allowRCUpgrades,omitempty"`

	// DryRun 指示是否启用演练模式；启用时不应用任何更改，仅输出将要执行的操作。
	// +optional
	DryRun *bool `json:"dryRun,omitempty"`

	// EtcdUpgrade 指示 kubeadm 在升级期间执行 etcd 升级，默认为 true。
	// +optional
	EtcdUpgrade *bool `json:"etcdUpgrade,omitempty"`

	// IgnorePreflightErrors 是升级过程中要忽略的预检错误列表，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	// +optional
	IgnorePreflightErrors []string `json:"ignorePreflightErrors,omitempty"`

	// PrintConfig 指定是否打印升级将使用的配置文件。
	// +optional
	PrintConfig *bool `json:"printConfig,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpgradeConfiguration 包含专用于 `kubeadm upgrade` 各子命令的选项。
type UpgradeConfiguration struct {
	metav1.TypeMeta `json:""`

	// Apply 保存专用于 `kubeadm upgrade apply` 命令的选项。
	// +optional
	Apply UpgradeApplyConfiguration `json:"apply,omitempty"`

	// Diff 保存专用于 `kubeadm upgrade diff` 命令的选项。
	// +optional
	Diff UpgradeDiffConfiguration `json:"diff,omitempty"`

	// Node 保存专用于 `kubeadm upgrade node` 命令的选项。
	// +optional
	Node UpgradeNodeConfiguration `json:"node,omitempty"`

	// Plan 保存专用于 `kubeadm upgrade plan` 命令的选项。
	// +optional
	Plan UpgradePlanConfiguration `json:"plan,omitempty"`

	// Timeouts 保存适用于 kubeadm 命令的各种超时设置。
	// +optional
	Timeouts *Timeouts `json:"timeouts,omitempty"`
}
