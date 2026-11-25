/*
Copyright 2019 The Kubernetes Authors.

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

package options

const (
	// APIServerAdvertiseAddress 参数设置 API 服务器对外公布的监听 IP 地址。指定 '0.0.0.0' 时使用默认网络接口的地址。
	APIServerAdvertiseAddress = "apiserver-advertise-address"

	// APIServerBindPort 参数设置 API 服务器的监听端口。
	APIServerBindPort = "apiserver-bind-port"

	// APIServerCertSANs 参数设置 API 服务器服务证书使用的额外主题备用名称（SAN），可以是 IP 地址或 DNS 名称。
	APIServerCertSANs = "apiserver-cert-extra-sans"

	// APIServerExtraArgs 参数以 <flagname>=<value> 形式设置传递给 API 服务器的额外参数，或覆盖默认参数。
	APIServerExtraArgs = "apiserver-extra-args"

	// CertificatesDir 参数设置证书的保存和读取路径。
	CertificatesDir = "cert-dir"

	// CfgPath 参数设置 kubeadm 配置文件路径。
	CfgPath = "config"

	// ControllerManagerExtraArgs 参数以 <flagname>=<value> 形式设置传递给 Controller Manager 的额外参数，或覆盖默认参数。
	ControllerManagerExtraArgs = "controller-manager-extra-args"

	// ControlPlaneEndpoint 参数设置控制平面的稳定 IP 地址或 DNS 名称。
	ControlPlaneEndpoint = "control-plane-endpoint"

	// DryRun 参数要求 kubeadm 不应用任何更改，仅输出将要执行的操作。
	DryRun = "dry-run"

	// FeatureGatesString 参数设置描述各项功能开关的 key=value 对。
	FeatureGatesString = "feature-gates"

	// IgnorePreflightErrors 参数设置其错误将显示为警告的检查列表，例如 'IsPrivilegedUser,Swap'。值为 'all' 时忽略所有检查的错误。
	IgnorePreflightErrors = "ignore-preflight-errors"

	// ImageRepository 参数设置拉取控制平面镜像的容器镜像仓库。
	ImageRepository = "image-repository"

	// KubeconfigDir 参数设置 kubeconfig 文件的保存路径。
	KubeconfigDir = "kubeconfig-dir"

	// KubeconfigPath 参数设置与集群通信时使用的 kubeconfig 文件。未设置时，将搜索一组标准位置中的现有 KubeConfig 文件。
	KubeconfigPath = "kubeconfig"

	// KubernetesVersion 参数设置控制平面的 Kubernetes 版本。
	KubernetesVersion = "kubernetes-version"

	// NetworkingDNSDomain 参数设置服务域名，例如 "myorg.internal"。
	NetworkingDNSDomain = "service-dns-domain"

	// NetworkingServiceSubnet 参数设置服务 VIP 的 IP 地址范围。
	NetworkingServiceSubnet = "service-cidr"

	// NetworkingPodSubnet 参数设置 Pod 网络的 IP 地址范围。设置后，控制平面将自动为每个节点分配 CIDR。
	NetworkingPodSubnet = "pod-network-cidr"

	// NodeCRISocket 参数设置要连接的 CRI 套接字。
	NodeCRISocket = "cri-socket"

	// NodeName 参数设置节点名称。
	NodeName = "node-name"

	// SchedulerExtraArgs 参数以 <flagname>=<value> 形式设置传递给 Scheduler 的额外参数，或覆盖默认参数。
	SchedulerExtraArgs = "scheduler-extra-args"

	// SkipTokenPrint 参数要求 kubeadm 跳过打印由 kubeadm init 生成的默认引导令牌。
	SkipTokenPrint = "skip-token-print"

	// TokenStr 参数在未提供对应值时，同时设置 discovery-token 和 tls-bootstrap-token。
	TokenStr = "token"

	// TokenTTL 参数设置令牌的生存时间。
	TokenTTL = "token-ttl"

	// TokenUsages 参数设置令牌用途。
	TokenUsages = "usages"

	// TokenGroups 参数设置令牌的认证组。
	TokenGroups = "groups"

	// TokenDescription 参数设置令牌描述。
	TokenDescription = "description"

	// TLSBootstrapToken 参数设置用于临时向 Kubernetes 控制平面认证并为本地创建的密钥对提交证书签名请求（CSR）的令牌。
	TLSBootstrapToken = "tls-bootstrap-token"

	// TokenDiscovery 参数设置用于验证从 API 服务器获取的集群信息的令牌（基于令牌的发现）。
	TokenDiscovery = "discovery-token"

	// TokenDiscoveryCAHash 参数要求 kubeadm 验证根 CA 公钥是否匹配此哈希（基于令牌的发现）。
	TokenDiscoveryCAHash = "discovery-token-ca-cert-hash"

	// TokenDiscoverySkipCAHash 参数要求 kubeadm 跳过 CA 哈希验证（基于令牌的发现）。
	TokenDiscoverySkipCAHash = "discovery-token-unsafe-skip-ca-verification"

	// FileDiscovery 参数设置用于加载集群信息的文件或 URL（基于文件的发现）。
	FileDiscovery = "discovery-file"

	// ControlPlane 参数要求 kubeadm 在此节点上创建新的控制平面实例。
	ControlPlane = "control-plane"

	// UploadCerts 参数要求 kubeadm 上传证书。
	UploadCerts = "upload-certs"

	// CertificateKey 参数设置用于加密和解密证书 Secret 的密钥。
	CertificateKey = "certificate-key"

	// SkipCertificateKeyPrint 参数要求 kubeadm 跳过打印 kubeadm init 用于加密证书的密钥。
	SkipCertificateKeyPrint = "skip-certificate-key-print"

	// Force 参数要求 kubeadm 执行重置、升级等操作时不请求确认。
	Force = "force"

	// CertificateRenewal 参数要求 kubeadm 在升级期间执行证书续期。
	CertificateRenewal = "certificate-renewal"

	// EtcdUpgrade 参数要求 kubeadm 在升级期间执行 etcd 升级。
	EtcdUpgrade = "etcd-upgrade"

	// Patches 参数设置保存 kubeadm 组件补丁的目录。
	Patches = "patches"

	// PrintManifest 参数要求 kubeadm 将插件清单打印到 STDOUT，而不是安装它们。
	PrintManifest = "print-manifest"

	// CleanupTmpDir 参数指示 reset 是否清理临时目录。
	CleanupTmpDir = "cleanup-tmp-dir"

	// AllowDeprecatedAPI 参数可用于允许使用已弃用的 API。
	AllowDeprecatedAPI = "allow-deprecated-api"

	// AllowExperimentalAPI 参数可用于允许使用实验性或仍在开发中的 API。
	AllowExperimentalAPI = "allow-experimental-api"

	// AllowRCUpgrades 参数允许升级到 Kubernetes 候选发布版本。
	AllowRCUpgrades = "allow-release-candidate-upgrades"

	// AllowExperimentalUpgrades 参数允许升级到 Kubernetes 的 alpha、beta 或候选发布版本。
	AllowExperimentalUpgrades = "allow-experimental-upgrades"

	// PrintConfig 指定是否打印升级将使用的集群配置。
	PrintConfig = "print-config"
)
