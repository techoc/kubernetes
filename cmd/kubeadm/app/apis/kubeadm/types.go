/*
Copyright 2016 The Kubernetes Authors.

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

package kubeadm

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InitConfiguration 包含仅用于 "kubeadm init" 运行时的字段。
// 集群级配置存储在 ClusterConfiguration 中。InitConfiguration 对象不会上传到集群中的
// kubeadm-config ConfigMap，只有 ClusterConfiguration 会上传。
type InitConfiguration struct {
	metav1.TypeMeta

	// ClusterConfiguration 保存集群级信息，并嵌入该结构体；该结构体也可单独编组或解组。
	// 将 InitConfiguration 编组为外部版本的字节时，此信息不会保留，
	// 因为这些 API 类型的外部版本使用 `json:"-"` tag。
	ClusterConfiguration `json:"-"`

	// BootstrapTokens 在 "kubeadm init" 时生效，描述要创建的一组 Bootstrap Token。
	BootstrapTokens []bootstraptokenv1.BootstrapToken

	// DryRun 指示是否启用空运行模式；启用时不应用任何更改，仅输出将要执行的操作。
	DryRun bool

	// NodeRegistration 保存将新控制平面节点注册到集群所需的字段。
	NodeRegistration NodeRegistrationOptions

	// LocalAPIEndpoint 表示部署在此控制平面节点上的 API 服务器实例端点。
	// 在高可用集群中，它不同于 ClusterConfiguration.ControlPlaneEndpoint：后者是将请求
	// 负载均衡到各 API 服务器的全局集群端点。此配置可自定义本地 API 服务器通告其可访问的
	// IP/DNS 名称和端口。默认情况下，kubeadm 尝试自动探测默认网卡的 IP；探测失败时需在此指定。
	LocalAPIEndpoint APIEndpoint

	// CertificateKey 设置加密密钥；在 uploadcerts init 阶段，证书和密钥会先加密再上传到集群 Secret。
	// 证书密钥是长度为 32 字节的 AES 密钥的十六进制编码字符串。
	CertificateKey string

	// SkipPhases 是命令执行期间要跳过的阶段列表。
	// 可运行 "kubeadm init --help" 查看阶段列表。
	// 命令行参数 "--skip-phases" 优先于此字段。
	SkipPhases []string

	// Patches 包含 "kubeadm init" 期间向 kubeadm 部署的组件应用补丁所需的选项。
	Patches *Patches

	// Timeouts 保存适用于 kubeadm 命令的各种超时。
	Timeouts *Timeouts
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterConfiguration 包含 kubeadm 集群的集群级配置。
type ClusterConfiguration struct {
	metav1.TypeMeta

	// ComponentConfigs 保存 kubeadm 已知的组件配置；从长期设计看，它只应存在于内部 kubeadm API 中。
	// +k8s:conversion-gen=false
	ComponentConfigs ComponentConfigMap

	// Etcd 保存 etcd 配置。
	Etcd Etcd

	// Networking 保存集群网络拓扑配置。
	Networking Networking

	// KubernetesVersion 是控制平面的目标版本。
	KubernetesVersion string

	// CIKubernetesVersion 是控制平面的目标 CI 版本，用于运行 CI 版本的 kubeadm。
	// +k8s:conversion-gen=false
	CIKubernetesVersion string

	// ControlPlaneEndpoint 设置控制平面的稳定 IP 地址或 DNS 名称；两者均可带可选 TCP 端口，
	// 且必须分别是有效 IP 地址或 RFC-1123 DNS 子域。
	// 未指定时使用 AdvertiseAddress + BindPort；已指定但未带 TCP 端口时使用 BindPort。
	// 常见用法：
	// 例如，在具有多个控制平面实例的集群中，应将其设置为控制平面实例前方外部负载均衡器的地址。
	// 例如，在强制回收节点的环境中，可用它为控制平面分配稳定的 DNS 名称。
	ControlPlaneEndpoint string

	// APIServer 保存 API 服务器控制平面组件的额外设置。
	APIServer APIServer

	// ControllerManager 保存控制器管理器控制平面组件的额外设置。
	ControllerManager ControlPlaneComponent

	// Scheduler 保存调度器控制平面组件的额外设置。
	Scheduler ControlPlaneComponent

	// DNS 定义集群中安装的 DNS 插件的选项。
	DNS DNS

	// Proxy 定义集群中安装的代理插件的选项。
	Proxy Proxy

	// CertificatesDir 指定存储或查找所有必需证书的目录。
	CertificatesDir string

	// ImageRepository 设置用于拉取镜像的容器镜像仓库。
	// 为空时默认使用 `registry.k8s.io`；若 Kubernetes 版本为 CI 构建（版本以 `ci/` 开头），
	// 控制平面组件和 kube-proxy 默认使用 `gcr.io/k8s-staging-ci-images`，其他镜像默认使用 `registry.k8s.io`。
	ImageRepository string

	// CIImageRepository 是 CI 生成的核心镜像的容器镜像仓库，用于运行 CI 构建的镜像。
	// +k8s:conversion-gen=false
	CIImageRepository string

	// FeatureGates 保存用户启用的 kubeadm 专属功能开关。
	FeatureGates map[string]bool

	// 集群名称，用于 kubeadm 生成的 kubeconfig 文件，并作为 kube-controller-manager 的 --cluster-name 参数值。
	// 默认值为 'kubernetes'。
	ClusterName string

	// EncryptionAlgorithm 保存用于密钥和证书的非对称加密算法类型。
	// 可选 "RSA-2048"（默认）、"RSA-3072"、"RSA-4096"、"ECDSA-P256" 或 "ECDSA-P384"。
	EncryptionAlgorithm EncryptionAlgorithmType

	// CertificateValidityPeriod 指定 kubeadm 生成的非 CA 证书的有效期。
	// 默认值为 8760h（365 天 × 24 小时 = 1 年）。
	CertificateValidityPeriod *metav1.Duration

	// CACertificateValidityPeriod 指定 kubeadm 生成的 CA 证书的有效期。
	// 默认值为 87600h（365 天 × 24 小时 × 10 = 10 年）。
	CACertificateValidityPeriod *metav1.Duration
}

// ControlPlaneComponent 保存集群控制平面组件的通用设置。
type ControlPlaneComponent struct {
	// ExtraArgs 是传递给控制平面组件的额外参数集。
	// 参数名是命令行中的参数名称，但不含前导连字符。额外参数会覆盖已有默认参数，允许重复。
	// 默认参数按字母数字顺序排序，额外参数不排序。
	ExtraArgs []Arg

	// ExtraVolumes 是挂载到控制平面组件的额外主机卷集。
	ExtraVolumes []HostPathMount

	// ExtraEnvs 是传递给控制平面组件的额外环境变量集。
	// 这些变量会覆盖已有环境变量，以及 kubeadm 默认添加的 *_proxy 环境变量。
	// +optional
	ExtraEnvs []EnvVar
}

// APIServer 保存集群中 API 服务器部署所需的设置。
type APIServer struct {
	ControlPlaneComponent

	// CertSANs 为 API 服务器签名证书设置额外的 Subject Alternative Name。
	CertSANs []string
}

// DNS 定义集群中应使用的 DNS 插件。
type DNS struct {
	// ImageMeta 允许自定义 DNS 插件使用的镜像。
	ImageMeta `json:""`

	// Disabled 指定是否在集群中禁用此插件。
	Disabled bool
}

// Proxy 定义集群中应使用的代理插件。
type Proxy struct {
	// Disabled 指定是否在集群中禁用此插件。
	Disabled bool
}

// ImageMeta 允许自定义不来源于 Kubernetes/Kubernetes 发布流程的组件镜像。
type ImageMeta struct {
	// ImageRepository 设置用于拉取镜像的容器镜像仓库。
	// 未设置时改用 ClusterConfiguration 中定义的 ImageRepository。
	ImageRepository string

	// ImageTag 允许指定镜像 tag；设置后，kubeadm 升级时不会自动更改上述组件的版本。
	ImageTag string

	// TODO: 根据用户反馈评估是否还需要 ImageName。
}

// APIEndpoint 包含部署在节点上的 API 服务器实例的组成元素。
type APIEndpoint struct {
	// AdvertiseAddress 设置 API 服务器通告的 IP 地址。
	AdvertiseAddress string

	// BindPort 设置 API 服务器绑定的安全端口，默认为 6443。
	BindPort int32
}

// NodeRegistrationOptions 保存通过 "kubeadm init" 或 "kubeadm join" 将新控制平面节点或节点注册到集群所需的字段。
type NodeRegistrationOptions struct {

	// Name 是此次 "kubeadm init" 或 "kubeadm join" 操作创建的 Node API 对象的 `.Metadata.Name` 字段。
	// 此字段也用作 kubelet 客户端证书中指向 API 服务器的 CommonName；未提供时默认为节点主机名。
	Name string

	// CRISocket 用于获取容器运行时信息，这些信息会注解到 Node API 对象中供后续复用。
	CRISocket string

	// Taints 指定 Node API 对象注册时应携带的污点。
	// 未设置（nil）时，控制平面节点默认带有 control-plane 污点；若不希望设置该污点，
	// 请在 YAML 文件中将此字段设为空切片，即 `taints: []`。此字段仅用于节点注册。
	Taints []v1.Taint

	// KubeletExtraArgs 将额外参数传递给 kubelet；参数经 kubeadm 在运行时写入的环境文件传给 kubelet 命令行。
	// 它们会覆盖 kubelet-config ConfigMap 中的通用基础配置，且解析时优先于默认参数。
	// 这些值仅对 kubeadm 所在节点有效。参数名不含前导连字符；额外参数会覆盖已有默认参数，允许重复。
	// 默认参数按字母数字顺序排序，额外参数不排序。
	KubeletExtraArgs []Arg

	// IgnorePreflightErrors 列出当前节点注册时要忽略的预检查错误，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	IgnorePreflightErrors []string

	// ImagePullPolicy 指定 kubeadm 在 "init" 和 "join" 操作期间拉取镜像的策略。
	// 可选 "Always"、"IfNotPresent" 或 "Never"；未设置时默认为 "IfNotPresent"，即仅在主机缺少所需镜像时拉取。
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSerial 指定 kubeadm 拉取镜像必须串行还是并行执行。
	ImagePullSerial *bool
}

// Networking 包含描述集群网络配置的字段。
type Networking struct {
	// ServiceSubnet 是 Kubernetes 服务使用的子网，默认为 "10.96.0.0/12"。
	ServiceSubnet string
	// PodSubnet 是 Pod 使用的子网。
	PodSubnet string
	// DNSDomain 是 Kubernetes 服务使用的 DNS 域，默认为 "cluster.local"。
	DNSDomain string
}

// Etcd 包含描述 etcd 配置的字段。
type Etcd struct {

	// Local 提供配置本地 etcd 实例的选项；Local 与 External 互斥。
	Local *LocalEtcd

	// External 描述如何连接外部 etcd 集群；Local 与 External 互斥。
	External *ExternalEtcd
}

// LocalEtcd 描述 kubeadm 应在本地运行 etcd 集群。
type LocalEtcd struct {
	// ImageMeta 允许自定义 etcd 容器镜像；指定自定义镜像会告知 kubeadm 升级跳过该用户管理镜像的升级。
	ImageMeta `json:""`

	// DataDir 是 etcd 存放数据的目录，默认为 "/var/lib/etcd"。
	DataDir string

	// ExtraArgs 是 etcd 在静态 Pod 中运行时接收的额外参数。
	// 参数名是命令行参数名称，但不含前导连字符；额外参数会覆盖已有默认参数，允许重复。
	// 默认参数按字母数字顺序排序，额外参数不排序。
	ExtraArgs []Arg

	// ExtraEnvs 是传递给控制平面组件的额外环境变量集。
	// 这些变量会覆盖已有环境变量，以及 kubeadm 默认添加的 *_proxy 环境变量。
	// +optional
	ExtraEnvs []EnvVar

	// ServerCertSANs 为 etcd 服务器签名证书设置额外的 Subject Alternative Name。
	ServerCertSANs []string
	// PeerCertSANs 为 etcd 对等节点签名证书设置额外的 Subject Alternative Name。
	PeerCertSANs []string
}

// ExternalEtcd 描述外部 etcd 集群。
type ExternalEtcd struct {
	// Endpoints 是 etcd 成员端点，使用外部 etcd 时必填。
	// 指定用于 etcd 通信的客户端 URL（通常为 gRPC 端点）。默认同时处理 gRPC 流量（etcd 主协议）
	// 和 HTTP 流量（指标、健康检查）；配置 HTTPEndpoints 后可分离两类流量，以提高安全性和性能。
	// 对应 etcd 的 --listen-client-urls 配置。
	Endpoints []string

	// HTTPEndpoints 是专用于 etcd 通信的 HTTP 端点。
	// 配置后，HTTP 流量（如 /metrics 和 /health 端点）与 Endpoints 处理的 gRPC 流量分离；
	// 这样可在不暴露主 gRPC 接口的情况下控制 HTTP 端点的访问。
	// 对应 etcd 的 --listen-client-http-urls 配置；未提供时，Endpoints 同时处理 gRPC 和 HTTP 流量。
	// +optional
	HTTPEndpoints []string

	// CAFile 是用于保护 etcd 通信的 SSL 证书颁发机构文件。
	CAFile string
	// CertFile 是用于保护 etcd 通信的 SSL 证书文件。
	CertFile string
	// KeyFile 是用于保护 etcd 通信的 SSL 密钥文件。
	KeyFile string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// JoinConfiguration 包含描述特定节点的字段。
type JoinConfiguration struct {
	metav1.TypeMeta

	// DryRun 指示是否启用空运行模式；启用时不应用任何更改，仅输出将要执行的操作。
	DryRun bool

	// NodeRegistration 保存将新控制平面节点注册到集群所需的字段。
	NodeRegistration NodeRegistrationOptions

	// CACertPath 是用于保护节点与控制平面通信的 SSL 证书颁发机构路径。
	// 默认为 "/etc/kubernetes/pki/ca.crt"。
	CACertPath string

	// Discovery 指定 kubelet 在 TLS 引导过程中使用的选项。
	Discovery Discovery

	// ControlPlane 定义要部署在加入节点上的额外控制平面实例；为 nil 时不部署。
	ControlPlane *JoinControlPlane

	// SkipPhases 是命令执行期间要跳过的阶段列表。
	// 可运行 "kubeadm join --help" 查看阶段列表；命令行参数 "--skip-phases" 优先于此字段。
	SkipPhases []string

	// Patches 包含 "kubeadm join" 期间向 kubeadm 部署的组件应用补丁所需的选项。
	Patches *Patches

	// Timeouts 保存适用于 kubeadm 命令的各种超时。
	Timeouts *Timeouts
}

// JoinControlPlane 包含描述要部署在加入节点上的额外控制平面实例的字段。
type JoinControlPlane struct {
	// LocalAPIEndpoint 表示要部署在此节点上的 API 服务器实例端点。
	LocalAPIEndpoint APIEndpoint

	// CertificateKey 是加入新控制平面节点时，证书从 Secret 下载后用于解密证书的密钥。
	// 对应的加密密钥位于 InitConfiguration 中；证书密钥是长度为 32 字节的 AES 密钥的十六进制编码字符串。
	CertificateKey string
}

// Discovery 指定 kubelet 在 TLS 引导过程中使用的选项。
type Discovery struct {
	// BootstrapToken 用于设置基于 Bootstrap Token 的发现选项；BootstrapToken 与 File 互斥。
	BootstrapToken *BootstrapTokenDiscovery

	// File 指定用于加载集群信息的 kubeconfig 文件或 URL；BootstrapToken 与 File 互斥。
	File *FileDiscovery

	// TLSBootstrapToken 是用于 TLS 引导的令牌。
	// 设置 .BootstrapToken 时，此字段默认取 .BootstrapToken.Token，但可覆盖。
	// 设置 .File 时，若 KubeConfigFile 不含其他身份验证信息，则必须设置此字段。
	TLSBootstrapToken string
}

// BootstrapTokenDiscovery 用于设置基于 Bootstrap Token 的发现选项。
type BootstrapTokenDiscovery struct {
	// Token 是用于验证从控制平面获取的集群信息的令牌。
	Token string

	// APIServerEndpoint 是用于获取信息的 API 服务器 IP 地址或域名。
	APIServerEndpoint string

	// CACertHashes 指定基于令牌发现时用于验证的公钥固定值集合；发现得到的根 CA 必须匹配其中一个值。
	// 指定空集合会禁用根 CA 固定验证，可能不安全。每个哈希采用 "<type>:<value>" 格式，
	// 目前仅支持 "sha256" 类型，值为 DER 编码 ASN.1 中 Subject Public Key Info（SPKI）对象的
	// SHA-256 哈希的十六进制编码，可使用 OpenSSL 等工具计算。
	CACertHashes []string

	// UnsafeSkipCAVerification 允许基于令牌发现时跳过 CACertHashes 的 CA 验证。
	// 其他节点可能冒充控制平面，因此这会削弱 kubeadm 的安全性。
	UnsafeSkipCAVerification bool
}

// FileDiscovery 用于指定从中加载集群信息的 kubeconfig 文件或 URL。
type FileDiscovery struct {
	// KubeConfigPath 指定用于加载集群信息的 kubeconfig 文件路径或 URL。
	KubeConfigPath string
}

// GetControlPlaneImageRepository 返回控制平面镜像（API、Controller Manager、Scheduler 和 Proxy）的镜像仓库名称。
// 用户请求 CI 构建区域中的特殊 Kubernetes 版本时，将使用 CI 镜像仓库覆盖该位置。
// （参见：kubeadmconstants.DefaultCIImageRepository）
func (cfg *ClusterConfiguration) GetControlPlaneImageRepository() string {
	if cfg.CIImageRepository != "" {
		return cfg.CIImageRepository
	}
	return cfg.ImageRepository
}

// HostPathMount 包含描述从主机挂载的卷的字段。
type HostPathMount struct {
	// Name 是卷在 Pod 模板中的名称。
	Name string
	// HostPath 是要挂载到 Pod 内的主机路径。
	HostPath string
	// MountPath 是 hostPath 挂载到 Pod 内的路径。
	MountPath string
	// ReadOnly 控制卷的写权限。
	ReadOnly bool
	// PathType 是 HostPath 的类型。
	PathType v1.HostPathType
}

// Patches 包含向 kubeadm 部署的组件应用补丁所需的选项。
type Patches struct {
	// Directory 是存放补丁文件的目录，文件命名为 "target[suffix][+patchtype].extension"。
	// 例如 "kube-apiserver0+merge.yaml" 或 "etcd.json"。"target" 可选
	// "kube-apiserver"、"kube-controller-manager"、"kube-scheduler"、"etcd"、"kubeletconfiguration"、"corednsdeployment"、"kubeproxydaemonset"。
	// "patchtype" 可选 "strategic"、"merge" 或 "json"，对应 kubectl 支持的补丁格式，默认 "patchtype" 为 "strategic"。
	// "extension" 必须为 "json" 或 "yaml"；"suffix" 是可选字符串，用于按字母数字顺序确定补丁的应用先后。
	Directory string
}

// DocumentMap 是一种在 YAML 文档与其 GVK 类型之间建立映射的便捷表示。
// +k8s:deepcopy-gen=false
type DocumentMap map[schema.GroupVersionKind][]byte

// ComponentConfig 保存已知的组件配置。
type ComponentConfig interface {
	// DeepCopy 应在原位置创建组件配置的新深拷贝。
	DeepCopy() ComponentConfig

	// Marshal 将配置编组为 YAML 文档，并以字节切片返回。
	Marshal() ([]byte, error)

	// Unmarshal 从文档映射加载配置；文档映射中无配置不视为错误。
	Unmarshal(docmap DocumentMap) error

	// Default 使用 kubeadm 推荐的默认值填充组件配置。
	Default(cfg *ClusterConfiguration, localAPIEndpoint *APIEndpoint, nodeRegOpts *NodeRegistrationOptions)

	// IsUserSupplied 指示组件配置是用户提供或修改的，还是由 kubeadm 生成的。
	IsUserSupplied() bool

	// SetUserSupplied 将组件配置的 "user supplied" 标志设置为 true 或 false。
	SetUserSupplied(userSupplied bool)

	// Mutate 允许在编组前对配置应用预定义修改。
	Mutate() error

	// Set 可用于设置 ComponentConfig 的内部配置。
	Set(interface{})

	// Get 可用于获取 ComponentConfig 的内部配置。
	Get() interface{}
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResetConfiguration 包含仅用于 "kubeadm reset" 运行时的字段。
type ResetConfiguration struct {
	metav1.TypeMeta

	// CertificatesDir 指定证书存储目录；指定后会在重置过程中清理该目录。
	CertificatesDir string

	// CleanupTmpDir 指定重置过程中是否清理 "/etc/kubernetes/tmp" 目录。
	CleanupTmpDir bool

	// CRISocket 用于获取容器运行时信息并删除容器。
	// 未通过参数或配置文件指定时，kubeadm 会尝试探测有效的 CRISocket。
	CRISocket string

	// DryRun 指示是否启用空运行模式；启用时不应用任何更改，仅输出将要执行的操作。
	DryRun bool

	// Force 指示 kubeadm 无需确认即可重置节点。
	Force bool

	// IgnorePreflightErrors 列出重置过程中要忽略的预检查错误，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	IgnorePreflightErrors []string

	// SkipPhases 是命令执行期间要跳过的阶段列表，可运行 "kubeadm reset phase --help" 查看。
	SkipPhases []string

	// UnmountFlags 列出 kubeadm 在 "reset" 期间卸载目录时可使用的 unmount2() 系统调用标志。
	// 可选 MNT_FORCE、MNT_DETACH、MNT_EXPIRE、UMOUNT_NOFOLLOW，默认为空列表。
	UnmountFlags []string

	// Timeouts 保存适用于 kubeadm 命令的各种超时。
	Timeouts *Timeouts
}

// UpgradeApplyConfiguration 包含 "kubeadm upgrade apply" 命令专属的可配置选项。
type UpgradeApplyConfiguration struct {
	// KubernetesVersion 是控制平面的目标版本。
	KubernetesVersion string

	// AllowExperimentalUpgrades 指示 kubeadm 将不稳定的 Kubernetes 版本列为升级选项，
	// 并允许升级到 Kubernetes 的 alpha、beta 或候选版本；默认为 false。
	AllowExperimentalUpgrades *bool

	// AllowRCUpgrades 指示 kubeadm 将 Kubernetes 候选版本列为升级选项，并允许升级到候选版本。
	AllowRCUpgrades *bool

	// CertificateRenewal 指示 kubeadm 在升级期间执行证书续期。
	CertificateRenewal *bool

	// DryRun 指示是否启用空运行模式；启用时不应用任何更改，仅输出将要执行的操作。
	DryRun *bool

	// EtcdUpgrade 指示 kubeadm 在升级期间执行 etcd 升级。
	EtcdUpgrade *bool

	// ForceUpgrade 指示 kubeadm 无需确认即可升级集群。
	ForceUpgrade *bool

	// IgnorePreflightErrors 列出升级过程中要忽略的预检查错误，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	IgnorePreflightErrors []string

	// Patches 包含 "kubeadm upgrade" 期间向 kubeadm 部署的组件应用补丁所需的选项。
	Patches *Patches

	// PrintConfig 指定是否打印升级将使用的配置文件。
	PrintConfig *bool

	// SkipPhases 是命令执行期间要跳过的阶段列表。
	// 注意：此字段目前对 "kubeadm upgrade apply" 无效，未来将支持。
	SkipPhases []string

	// ImagePullPolicy 指定 kubeadm 在 "upgrade apply" 操作期间拉取镜像的策略。
	// 可选 "Always"、"IfNotPresent" 或 "Never"；未设置时默认为 "IfNotPresent"，即仅在主机缺少所需镜像时拉取。
	ImagePullPolicy v1.PullPolicy

	// ImagePullSerial 指定 kubeadm 拉取镜像必须串行还是并行执行。
	ImagePullSerial *bool
}

// UpgradeDiffConfiguration 包含 "kubeadm upgrade diff" 命令专属的可配置选项。
type UpgradeDiffConfiguration struct {
	// KubernetesVersion 是控制平面的目标版本。
	KubernetesVersion string

	// DiffContextLines 是差异输出中的上下文行数。
	DiffContextLines int
}

// UpgradeNodeConfiguration 包含 "kubeadm upgrade node" 命令专属的可配置选项。
type UpgradeNodeConfiguration struct {
	// CertificateRenewal 指示 kubeadm 在升级期间执行证书续期。
	CertificateRenewal *bool

	// DryRun 指示是否启用空运行模式；启用时不应用任何更改，仅输出将要执行的操作。
	DryRun *bool

	// EtcdUpgrade 指示 kubeadm 在升级期间执行 etcd 升级。
	EtcdUpgrade *bool

	// IgnorePreflightErrors 列出升级过程中要忽略的预检查错误，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	IgnorePreflightErrors []string

	// SkipPhases 是命令执行期间要跳过的阶段列表，可运行 "kubeadm upgrade node phase --help" 查看。
	SkipPhases []string

	// Patches 包含 "kubeadm upgrade" 期间向 kubeadm 部署的组件应用补丁所需的选项。
	Patches *Patches

	// ImagePullPolicy 指定 kubeadm 在 "upgrade node" 操作期间拉取镜像的策略。
	// 可选 "Always"、"IfNotPresent" 或 "Never"；未设置时默认为 "IfNotPresent"，即仅在主机缺少所需镜像时拉取。
	ImagePullPolicy v1.PullPolicy

	// ImagePullSerial 指定 kubeadm 拉取镜像必须串行还是并行执行。
	ImagePullSerial *bool
}

// UpgradePlanConfiguration 包含 "kubeadm upgrade plan" 命令专属的可配置选项。
type UpgradePlanConfiguration struct {
	// KubernetesVersion 是控制平面的目标版本。
	// +optional
	KubernetesVersion string

	// AllowExperimentalUpgrades 指示 kubeadm 将不稳定的 Kubernetes 版本列为升级选项，
	// 并允许升级到 Kubernetes 的 alpha、beta 或候选版本；默认为 false。
	// +optional
	AllowExperimentalUpgrades *bool

	// AllowRCUpgrades 指示 kubeadm 将 Kubernetes 候选版本列为升级选项，并允许升级到候选版本。
	AllowRCUpgrades *bool

	// DryRun 指示是否启用空运行模式；启用时不应用任何更改，仅输出将要执行的操作。
	DryRun *bool

	// EtcdUpgrade 指示 kubeadm 在升级期间执行 etcd 升级。
	EtcdUpgrade *bool

	// IgnorePreflightErrors 列出升级过程中要忽略的预检查错误，例如 'IsPrivilegedUser,Swap'。
	// 值为 'all' 时忽略所有检查的错误。
	IgnorePreflightErrors []string

	// PrintConfig 指定是否打印升级将使用的配置文件。
	PrintConfig *bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// UpgradeConfiguration 包含 "kubeadm upgrade" 子命令专属的选项。
type UpgradeConfiguration struct {
	metav1.TypeMeta

	// Apply 保存 "kubeadm upgrade apply" 命令专属的选项。
	Apply UpgradeApplyConfiguration

	// Diff 保存 "kubeadm upgrade diff" 命令专属的选项。
	Diff UpgradeDiffConfiguration

	// Node 保存 "kubeadm upgrade node" 命令专属的选项。
	Node UpgradeNodeConfiguration

	// Plan 保存 "kubeadm upgrade plan" 命令专属的选项。
	Plan UpgradePlanConfiguration

	// Timeouts 保存适用于 kubeadm 命令的各种超时。
	Timeouts *Timeouts
}

const (
	// UnmountFlagMNTForce 表示标志 "MNT_FORCE"。
	UnmountFlagMNTForce = "MNT_FORCE"
	// UnmountFlagMNTDetach 表示标志 "MNT_DETACH"。
	UnmountFlagMNTDetach = "MNT_DETACH"
	// UnmountFlagMNTExpire 表示标志 "MNT_EXPIRE"。
	UnmountFlagMNTExpire = "MNT_EXPIRE"
	// UnmountFlagUmountNoFollow 表示标志 "UMOUNT_NOFOLLOW"。
	UnmountFlagUmountNoFollow = "UMOUNT_NOFOLLOW"
)

// ComponentConfigMap 是 API 组名（GVK 中的组名）与 ComponentConfig 之间的映射。
type ComponentConfigMap map[string]ComponentConfig

// ArgMergeMethod 定义同名参数的合并方式。
type ArgMergeMethod string

const (
	// ArgMergeMethodAppend 将新值追加到已有参数值。
	ArgMergeMethodAppend ArgMergeMethod = "append"
	// ArgMergeMethodPrepend 将新值插入已有参数值之前。
	ArgMergeMethodPrepend ArgMergeMethod = "prepend"
)

// Arg 表示带名称和值的参数。
// MergeMethod 定义此参数与同名基础参数的合并方式；未指定时替换基础参数。
// 若多个覆盖参数同名且定义了合并方式，则使用最后一个。
// 若覆盖参数定义了合并方式但不存在同名基础参数，则忽略该覆盖参数。
type Arg struct {
	// Name 是参数名，匹配时按完整名称区分大小写。
	Name string
	// Value 是参数值；是否允许独立值由使用方决定。
	Value string
	// MergeMethod 指定与同名基础参数的合并方式；未设置时替换基础参数。
	MergeMethod ArgMergeMethod
}

// EnvVar 表示容器中存在的环境变量。
type EnvVar struct {
	v1.EnvVar
}

// EncryptionAlgorithmType 可定义非对称加密算法类型。
type EncryptionAlgorithmType string

const (
	// EncryptionAlgorithmECDSAP256 定义使用 P-256 曲线的 ECDSA 加密算法类型。
	EncryptionAlgorithmECDSAP256 EncryptionAlgorithmType = "ECDSA-P256"
	// EncryptionAlgorithmECDSAP384 定义使用 P-384 曲线的 ECDSA 加密算法类型。
	EncryptionAlgorithmECDSAP384 EncryptionAlgorithmType = "ECDSA-P384"
	// EncryptionAlgorithmRSA2048 定义密钥长度为 2048 位的 RSA 加密算法类型。
	EncryptionAlgorithmRSA2048 EncryptionAlgorithmType = "RSA-2048"
	// EncryptionAlgorithmRSA3072 定义密钥长度为 3072 位的 RSA 加密算法类型。
	EncryptionAlgorithmRSA3072 EncryptionAlgorithmType = "RSA-3072"
	// EncryptionAlgorithmRSA4096 定义密钥长度为 4096 位的 RSA 加密算法类型。
	EncryptionAlgorithmRSA4096 EncryptionAlgorithmType = "RSA-4096"
)

// Timeouts 保存适用于 kubeadm 命令的各种超时。
type Timeouts struct {
	// ControlPlaneComponentHealthCheck 是 "kubeadm init" 和 "kubeadm join" 期间等待 API 服务器等控制平面组件健康的时长。
	ControlPlaneComponentHealthCheck *metav1.Duration

	// KubeletHealthCheck 是 "kubeadm init" 和 "kubeadm join" 期间等待 kubelet 健康的时长。
	KubeletHealthCheck *metav1.Duration

	// KubernetesAPICall 是等待 kubeadm 客户端完成 API 服务器请求的时长，适用于 GET、POST 等所有方法。
	KubernetesAPICall *metav1.Duration

	// EtcdAPICall 是等待 kubeadm 的 etcd 客户端完成 etcd 集群请求的时长。
	EtcdAPICall *metav1.Duration

	// TLSBootstrap 是等待 kubelet 为加入节点完成 TLS 引导的时长。
	TLSBootstrap *metav1.Duration

	// Discovery 是等待 kubeadm 为加入节点验证 API 服务器身份的时长。
	Discovery *metav1.Duration

	// UpgradeManifests 是升级静态 Pod 清单的超时。
	UpgradeManifests *metav1.Duration
}
