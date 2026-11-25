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

package constants

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
	componentversion "k8s.io/component-base/version"
	netutils "k8s.io/utils/net"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// KubernetesDir 是 Kubernetes 用于存放各类配置文件的目录。
	KubernetesDir = "/etc/kubernetes"
	// ManifestsSubDirName 定义存放清单的目录名。
	ManifestsSubDirName = "manifests"
	// TempDir 定义 kubeadm 的临时目录。
	// 应与 KubernetesDir 拼接使用。
	TempDir = "tmp"

	// CertificateBackdate 定义 kubeadm 生成的 CA 证书在 notBefore 上回溯的时间量。
	CertificateBackdate = time.Minute * 5
	// CertificateValidityPeriod 定义 kubeadm 生成的所有已签名证书的有效期。
	CertificateValidityPeriod = time.Hour * 24 * 365
	// CACertificateValidityPeriod 定义 kubeadm 生成的所有已签名 CA 证书的有效期。
	CACertificateValidityPeriod = time.Hour * 24 * 365 * 10

	// DefaultCertificateDir 定义默认证书目录。
	DefaultCertificateDir = "pki"

	// CACertAndKeyBaseName 定义证书颁发机构证书和密钥的基础名称。
	CACertAndKeyBaseName = "ca"
	// CACertName 定义 CA 证书名称。
	CACertName = "ca.crt"
	// CAKeyName 定义 CA 密钥名称。
	CAKeyName = "ca.key"

	// APIServerCertAndKeyBaseName 定义 API 服务器证书和密钥的基础名称。
	APIServerCertAndKeyBaseName = "apiserver"
	// APIServerCertName 定义 API 服务器证书名称。
	APIServerCertName = "apiserver.crt"
	// APIServerKeyName 定义 API 服务器密钥名称。
	APIServerKeyName = "apiserver.key"
	// APIServerCertCommonName 定义 API 服务器证书的通用名称（CN）。
	APIServerCertCommonName = "kube-apiserver"

	// APIServerKubeletClientCertAndKeyBaseName 定义 kubelet 客户端证书和密钥的基础名称。
	APIServerKubeletClientCertAndKeyBaseName = "apiserver-kubelet-client"
	// APIServerKubeletClientCertName 定义 kubelet 客户端证书名称。
	APIServerKubeletClientCertName = "apiserver-kubelet-client.crt"
	// APIServerKubeletClientKeyName 定义 kubelet 客户端密钥名称。
	APIServerKubeletClientKeyName = "apiserver-kubelet-client.key"
	// APIServerKubeletClientCertCommonName 定义 kubelet 客户端证书的通用名称（CN）。
	APIServerKubeletClientCertCommonName = "kube-apiserver-kubelet-client"

	// EtcdCACertAndKeyBaseName 定义 etcd CA 证书和密钥的基础名称。
	EtcdCACertAndKeyBaseName = "etcd/ca"
	// EtcdCACertName 定义 etcd CA 证书名称。
	EtcdCACertName = "etcd/ca.crt"
	// EtcdCAKeyName 定义 etcd CA 密钥名称。
	EtcdCAKeyName = "etcd/ca.key"

	// EtcdServerCertAndKeyBaseName 定义 etcd 服务器证书和密钥的基础名称。
	EtcdServerCertAndKeyBaseName = "etcd/server"
	// EtcdServerCertName 定义 etcd 服务器证书名称。
	EtcdServerCertName = "etcd/server.crt"
	// EtcdServerKeyName 定义 etcd 服务器密钥名称。
	EtcdServerKeyName = "etcd/server.key"

	// EtcdListenClientPort 定义 etcd 监听客户端流量的端口。
	EtcdListenClientPort = 2379
	// EtcdMetricsPort 定义获取 etcd 指标和健康状态的端口。
	EtcdMetricsPort = 2381

	// EtcdPeerCertAndKeyBaseName 定义 etcd 对等证书和密钥的基础名称。
	EtcdPeerCertAndKeyBaseName = "etcd/peer"
	// EtcdPeerCertName 定义 etcd 对等证书名称。
	EtcdPeerCertName = "etcd/peer.crt"
	// EtcdPeerKeyName 定义 etcd 对等密钥名称。
	EtcdPeerKeyName = "etcd/peer.key"

	// EtcdListenPeerPort 定义 etcd 监听对等流量的端口。
	EtcdListenPeerPort = 2380

	// EtcdHealthcheckClientCertAndKeyBaseName 定义 etcd 健康检查客户端证书和密钥的基础名称。
	EtcdHealthcheckClientCertAndKeyBaseName = "etcd/healthcheck-client"
	// EtcdHealthcheckClientCertName 定义 etcd 健康检查客户端证书名称。
	EtcdHealthcheckClientCertName = "etcd/healthcheck-client.crt"
	// EtcdHealthcheckClientKeyName 定义 etcd 健康检查客户端密钥名称。
	EtcdHealthcheckClientKeyName = "etcd/healthcheck-client.key"
	// EtcdHealthcheckClientCertCommonName 定义 etcd 健康检查客户端证书的通用名称（CN）。
	EtcdHealthcheckClientCertCommonName = "kube-etcd-healthcheck-client"

	// APIServerEtcdClientCertAndKeyBaseName 定义 API 服务器的 etcd 客户端证书和密钥的基础名称。
	APIServerEtcdClientCertAndKeyBaseName = "apiserver-etcd-client"
	// APIServerEtcdClientCertName 定义 API 服务器的 etcd 客户端证书名称。
	APIServerEtcdClientCertName = "apiserver-etcd-client.crt"
	// APIServerEtcdClientKeyName 定义 API 服务器的 etcd 客户端密钥名称。
	APIServerEtcdClientKeyName = "apiserver-etcd-client.key"
	// APIServerEtcdClientCertCommonName 定义 API 服务器的 etcd 客户端证书通用名称（CN）。
	APIServerEtcdClientCertCommonName = "kube-apiserver-etcd-client"

	// ServiceAccountKeyBaseName 定义服务账号密钥的基础名称。
	ServiceAccountKeyBaseName = "sa"
	// ServiceAccountPublicKeyName 定义服务账号公钥名称。
	ServiceAccountPublicKeyName = "sa.pub"
	// ServiceAccountPrivateKeyName 定义服务账号私钥名称。
	ServiceAccountPrivateKeyName = "sa.key"

	// FrontProxyCACertAndKeyBaseName 定义前置代理 CA 证书和密钥的基础名称。
	FrontProxyCACertAndKeyBaseName = "front-proxy-ca"
	// FrontProxyCACertName 定义前置代理 CA 证书名称。
	FrontProxyCACertName = "front-proxy-ca.crt"
	// FrontProxyCAKeyName 定义前置代理 CA 密钥名称。
	FrontProxyCAKeyName = "front-proxy-ca.key"

	// FrontProxyClientCertAndKeyBaseName 定义前置代理证书和密钥的基础名称。
	FrontProxyClientCertAndKeyBaseName = "front-proxy-client"
	// FrontProxyClientCertName 定义前置代理证书名称。
	FrontProxyClientCertName = "front-proxy-client.crt"
	// FrontProxyClientKeyName 定义前置代理密钥名称。
	FrontProxyClientKeyName = "front-proxy-client.key"
	// FrontProxyClientCertCommonName 定义前置代理证书的通用名称。
	FrontProxyClientCertCommonName = "front-proxy-client" // 用作 subject.commonname 属性（CN）

	// AdminKubeConfigFileName 定义供集群管理员使用的 kubeconfig 文件名。
	AdminKubeConfigFileName = "admin.conf"
	// SuperAdminKubeConfigFileName 定义供集群超级管理员使用的 kubeconfig 文件名。
	SuperAdminKubeConfigFileName = "super-admin.conf"

	// KubeletBootstrapKubeConfigFileName 定义 kubelet 用于执行 TLS 引导并获取唯一凭据的 kubeconfig 文件名。
	KubeletBootstrapKubeConfigFileName = "bootstrap-kubelet.conf"

	// KubeletKubeConfigFileName 定义控制平面 kubelet 用于连接 API 服务器的 kubeconfig 文件名。
	KubeletKubeConfigFileName = "kubelet.conf"
	// ControllerManagerKubeConfigFileName 定义控制管理器的 kubeconfig 文件名。
	ControllerManagerKubeConfigFileName = "controller-manager.conf"
	// SchedulerKubeConfigFileName 定义调度器的 kubeconfig 文件名。
	SchedulerKubeConfigFileName = "scheduler.conf"

	// Kubernetes 核心授权系统中一些知名的用户、组、角色和 ClusterRoleBinding。

	// ControllerManagerUser 定义 controller-manager 认证时使用的知名用户。
	ControllerManagerUser = "system:kube-controller-manager"
	// SchedulerUser 定义调度器认证时使用的知名用户。
	SchedulerUser = "system:kube-scheduler"
	// NodesUserPrefix 定义 Node 授权器要求的用户名前缀。
	NodesUserPrefix = "system:node:"
	// SystemPrivilegedGroup 定义 API 服务器使用的知名组；该组默认也是超级用户组。
	// 即绑定到 cluster-admin ClusterRole。
	SystemPrivilegedGroup = "system:masters"
	// NodesGroup 定义所有节点所属的知名组。
	NodesGroup = "system:nodes"
	// NodeBootstrapTokenAuthGroup 指定节点引导令牌应被认证到哪个组。
	NodeBootstrapTokenAuthGroup = "system:bootstrappers:kubeadm:default-node-token"
	// KubeProxyClusterRoleName 设置 kube-proxy ClusterRole 的名称。
	KubeProxyClusterRoleName = "system:node-proxier"
	// NodeBootstrapperClusterRoleName 定义允许提交 CSR 的自动引导 ClusterRole 名称。
	NodeBootstrapperClusterRoleName = "system:node-bootstrapper"
	// CSRAutoApprovalClusterRoleName 定义让 csrapprover 控制器自动批准 CSR 的自动引导 ClusterRole 名称。
	// 从 v1.8 起，CSRAutoApprovalClusterRoleName 会在 API 服务器启动时自动创建。
	CSRAutoApprovalClusterRoleName = "system:certificates.k8s.io:certificatesigningrequests:nodeclient"
	// NodeSelfCSRAutoApprovalClusterRoleName 定义默认 1.8 RBAC 策略中的角色，用于自动批准自动轮换的节点证书 CSR。
	NodeSelfCSRAutoApprovalClusterRoleName = "system:certificates.k8s.io:certificatesigningrequests:selfnodeclient"
	// NodesClusterRoleBinding 定义将权限过大的 system:node ClusterRole
	// 绑定到 system:nodes 组的知名 ClusterRoleBinding。kubeadm 使用 Node Authorizer，
	// 因此该绑定存在时，会移除其中的 system:nodes 组主体。
	NodesClusterRoleBinding = "system:node"

	// KubeletBaseConfigMapRole 定义基础 kubelet 配置 ConfigMap。
	KubeletBaseConfigMapRole = "kubeadm:kubelet-config"
	// KubeProxyClusterRoleBindingName 设置 kube-proxy ClusterRoleBinding 的名称。
	KubeProxyClusterRoleBindingName = "kubeadm:node-proxier"
	// NodeKubeletBootstrap 定义允许 kubelet 提交 CSR 的 ClusterRoleBinding 名称。
	NodeKubeletBootstrap = "kubeadm:kubelet-bootstrap"
	// GetNodesClusterRoleName 定义用于获取节点的 ClusterRole 和 ClusterRoleBinding 名称。
	GetNodesClusterRoleName = "kubeadm:get-nodes"
	// NodeAutoApproveBootstrapClusterRoleBinding 定义让 csrapprover 批准节点 CSR 的 ClusterRoleBinding 名称。
	NodeAutoApproveBootstrapClusterRoleBinding = "kubeadm:node-autoapprove-bootstrap"
	// NodeAutoApproveCertificateRotationClusterRoleBinding 定义让 csrapprover 批准节点自动轮换 CSR 的 ClusterRoleBinding 名称。
	NodeAutoApproveCertificateRotationClusterRoleBinding = "kubeadm:node-autoapprove-certificate-rotation"
	// ClusterAdminsGroupAndClusterRoleBinding 定义 kubeadm 生成的集群管理员凭据所用组的名称，
	// 以及将该组绑定到内置 cluster-admin ClusterRole 的 ClusterRoleBinding 名称。
	ClusterAdminsGroupAndClusterRoleBinding = "kubeadm:cluster-admins"

	// KubeletAPIAdminClusterRoleBindingName 定义 API 服务器 kubelet 客户端的 ClusterRoleBinding 名称。
	KubeletAPIAdminClusterRoleBindingName = "kubeadm:apiserver-kubelet-client"
	// KubeletAPIAdminClusterRoleName 定义用于访问 kubelet API 的内置 ClusterRole 名称。
	KubeletAPIAdminClusterRoleName = "system:kubelet-api-admin"

	// KubernetesAPICallTimeout 指定 kubeadm 等待 API 调用的时长。
	KubernetesAPICallTimeout = 1 * time.Minute
	// KubernetesAPICallRetryInterval 定义 kubeadm 重试失败的 API 操作前的等待时长。
	KubernetesAPICallRetryInterval = 500 * time.Millisecond

	// DiscoveryTimeout 指定 kubeadm 的默认发现超时（JoinConfiguration 未指定时使用）。
	DiscoveryTimeout = 5 * time.Minute
	// DiscoveryRetryInterval 指定 kubeadm 发现控制平面时重试连接前的等待时长。
	DiscoveryRetryInterval = 5 * time.Second

	// TLSBootstrapTimeout 指定 kubeadm 等待 kubelet 完成 TLS Bootstrap 的时长。
	TLSBootstrapTimeout = 5 * time.Minute
	// TLSBootstrapRetryInterval 指定 kubeadm 重试 TLS Bootstrap 检查前的等待时长。
	TLSBootstrapRetryInterval = 1 * time.Second

	// EtcdAPICallTimeout 指定等待 etcd API 请求完成的时长。
	EtcdAPICallTimeout = 2 * time.Minute
	// EtcdAPICallRetryInterval 定义重试 etcd API 请求的频率。
	EtcdAPICallRetryInterval = 500 * time.Millisecond

	// ControlPlaneComponentHealthCheckTimeout 指定控制平面组件健康检查的默认超时。
	ControlPlaneComponentHealthCheckTimeout = 4 * time.Minute

	// KubeletHealthCheckTimeout 指定 kubelet 的默认超时。
	KubeletHealthCheckTimeout = 4 * time.Minute

	// UpgradeManifestsTimeout 指定升级静态 Pod 清单的默认超时。
	UpgradeManifestsTimeout = 5 * time.Minute

	// PullImageRetry 指定 ContainerRuntime 拉取镜像失败时的重试次数。
	PullImageRetry = 5
	// RemoveContainerRetry 指定 ContainerRuntime 删除容器失败时的重试次数。
	RemoveContainerRetry = 5

	// MinimumAddressesInServiceSubnet 定义 Service 子网至少应容纳的节点数。
	// 至少需要十个地址，因为 DNS 服务始终使用第十个集群 clusterIP。
	MinimumAddressesInServiceSubnet = 10

	// MaximumBitsForServiceSubnet 定义 Service 子网可用的最大位数。
	// 例如值为 20 时，IPv4 最大支持 /12，IPv6 最大支持 /108。
	// 但 /108 到 /112 之间的值会因底层分配逻辑的限制而收敛为 /112。
	// TODO：https://github.com/kubernetes/enhancements/pull/1881
	MaximumBitsForServiceSubnet = 20

	// MinimumAddressesInPodSubnet 定义集群中最少的 Pod 地址数。
	// Pod 地址需要多于 Service 地址；IPv4 /28 或 IPv6 /128 子网提供 14 个可用地址。
	MinimumAddressesInPodSubnet = 14

	// PodSubnetNodeMaskMaxDiff 受限于 16，因为核心代码存在未压缩 IP 位图问题：
	// xref: #44918
	// 节点子网掩码长度不得超过 Pod 子网掩码长度加 16。
	PodSubnetNodeMaskMaxDiff = 16

	// DefaultCertTokenDuration 指定上传证书所用令牌的默认有效期。
	// 默认行为为 2 小时。
	DefaultCertTokenDuration = 2 * time.Hour

	// CertificateKeySize 指定 uploadcerts 阶段用于加密证书的密钥长度。
	CertificateKeySize = 32

	// LabelNodeRoleControlPlane 指定节点承载控制平面组件。
	LabelNodeRoleControlPlane = "node-role.kubernetes.io/control-plane"

	// LabelExcludeFromExternalLB 可设置在节点上，以将该节点排除在外部负载均衡器之外。
	// 控制平面节点添加此标签是为了保持旧行为的向后兼容性。
	LabelExcludeFromExternalLB = "node.kubernetes.io/exclude-from-external-load-balancers"

	// KubeadmConfigConfigMap 指定在 kube-system 命名空间中存放 `kubeadm init` 配置的 ConfigMap。
	KubeadmConfigConfigMap = "kubeadm-config"

	// ClusterConfigurationConfigMapKey 指定存放集群配置的 ConfigMap 键。
	ClusterConfigurationConfigMapKey = "ClusterConfiguration"

	// KubeProxyConfigMap 指定在 kube-system 命名空间中存放 kube-proxy 配置的 ConfigMap。
	KubeProxyConfigMap = "kube-proxy"

	// KubeProxyConfigMapKey 指定存放 kube-proxy 组件配置的 ConfigMap 键。
	KubeProxyConfigMapKey = "config.conf"

	// KubeletBaseConfigurationConfigMap 指定在 kube-system 命名空间中存放 kubelet 初始远程配置的 ConfigMap。
	KubeletBaseConfigurationConfigMap = "kubelet-config"

	// KubeletBaseConfigurationConfigMapKey 指定存放 kubelet 初始远程配置的 ConfigMap 键。
	KubeletBaseConfigurationConfigMapKey = "kubelet"

	// KubeletRunDirectory 指定存放 kubelet 运行时信息的目录。
	KubeletRunDirectory = "/var/lib/kubelet"

	// KubeletConfigurationFileName 指定节点上存放 kubelet 初始远程配置的文件名。
	// 该文件应位于 KubeletRunDirectory 下。
	KubeletConfigurationFileName = "config.yaml"

	// KubeletInstanceConfigurationFileName 是写入所有节点的 kubelet 实例配置文件名。
	// 该文件应位于 KubeletRunDirectory 下。
	KubeletInstanceConfigurationFileName = "instance-config.yaml"

	// KubeletEnvFileName 是 "kubeadm init" 在运行时写入的文件名。kubeadm 可通过该接口
	// 根据运行时环境有条件地定制部分 kubelet 参数；配置文件中的参数也可能经此文件传递。
	// "kubeadm init" 写入一个名为 ${KubeletEnvFileVariableName} 的变量。
	// 该文件应位于 KubeletRunDirectory 下。
	KubeletEnvFileName = "kubeadm-flags.env"

	// KubeletEnvFileVariableName 指定 "kubeadm init" 在 KubeletEnvFile 中写入值的 shell 脚本变量名。
	KubeletEnvFileVariableName = "KUBELET_KUBEADM_ARGS"

	// KubeletHealthzPort 是 kubelet healthz 端点的端口。
	KubeletHealthzPort = 10248

	// MinExternalEtcdVersion 表示 kubeadm 支持的最低外部 etcd 版本。
	MinExternalEtcdVersion = "3.5.24-0"

	// DefaultEtcdVersion 表示 kubeadm 使用的默认 etcd 版本。
	DefaultEtcdVersion = "3.7.0-0"

	// Etcd 定义内部引用 etcd 组件时使用的变量。
	Etcd = "etcd"
	// KubeAPIServer 定义内部引用 kube-apiserver 组件时使用的变量。
	KubeAPIServer = "kube-apiserver"
	// KubeControllerManager 定义内部引用 kube-controller-manager 组件时使用的变量。
	KubeControllerManager = "kube-controller-manager"
	// KubeScheduler 定义内部引用 kube-scheduler 组件时使用的变量。
	KubeScheduler = "kube-scheduler"
	// KubeProxy 定义内部引用 kube-proxy 组件时使用的变量。
	KubeProxy = "kube-proxy"
	// CoreDNS 定义内部引用 CoreDNS 组件时使用的变量。
	CoreDNS = "CoreDNS"
	// Kubelet 定义内部引用 kubelet 时使用的变量。
	Kubelet = "kubelet"
	// Kubeadm 定义内部引用 kubeadm 组件时使用的变量。
	Kubeadm = "kubeadm"

	// KubeCertificatesVolumeName 指定向控制平面组件注入证书的 Volume 名称。
	// 可以是 hostPath Volume，也可以是 projected 一体化 Volume。
	KubeCertificatesVolumeName = "k8s-certs"

	// KubeConfigVolumeName 指定向控制平面组件注入 kubeconfig 的 Volume 名称，用于安全连接 API 服务器（如适用）。
	KubeConfigVolumeName = "kubeconfig"

	// DefaultCIImageRepository 指向 CI 构建任务上传镜像的镜像仓库。
	DefaultCIImageRepository = "gcr.io/k8s-staging-ci-images"

	// CoreDNSConfigMap 指定在 kube-system 命名空间中存放 CoreDNS 配置的 ConfigMap。
	CoreDNSConfigMap = "coredns"

	// CoreDNSDeploymentName 指定 CoreDNS 插件的 Deployment 名称。
	CoreDNSDeploymentName = "coredns"

	// CoreDNSImageName 指定 CoreDNS 插件的镜像名称。
	CoreDNSImageName = "coredns"

	// CoreDNSVersion 是启用 CoreDNS 插件时部署的 CoreDNS 版本。
	CoreDNSVersion = "v1.14.6"

	// ClusterConfigurationKind 是 ClusterConfiguration 结构体的字符串 kind 值。
	ClusterConfigurationKind = "ClusterConfiguration"

	// InitConfigurationKind 是 InitConfiguration 结构体的字符串 kind 值。
	InitConfigurationKind = "InitConfiguration"

	// JoinConfigurationKind 是 JoinConfiguration 结构体的字符串 kind 值。
	JoinConfigurationKind = "JoinConfiguration"

	// ResetConfigurationKind 是 ResetConfiguration 结构体的字符串 kind 值。
	ResetConfigurationKind = "ResetConfiguration"

	// YAMLDocumentSeparator 是 YAML 文档分隔符。
	// TODO：为这个常量寻找更合适的位置。
	YAMLDocumentSeparator = "---\n"

	// CIKubernetesVersionPrefix 是 CI Kubernetes 版本前缀。
	CIKubernetesVersionPrefix = "ci/"

	// DefaultAPIServerBindAddress 是 API 服务器的默认监听地址。
	DefaultAPIServerBindAddress = "0.0.0.0"

	// ControlPlaneNumCPU 是控制平面所需的 CPU 核数。
	ControlPlaneNumCPU = 2

	// ControlPlaneMem 是控制平面所需的内存兆字节数。
	// 低于该内存容量时，难以运行稳定的控制平面。
	ControlPlaneMem = 1700

	// KubeadmCertsSecret 指定在 kube-system 命名空间中存放证书的 Secret。
	KubeadmCertsSecret = "kubeadm-certs"

	// KubeletPort 是每台主机上 kubelet 服务的默认端口。
	// 可在启动时通过参数覆盖。
	KubeletPort = 10250
	// KubeSchedulerPort 是调度器状态服务的默认端口。
	// 可在启动时通过参数覆盖。
	KubeSchedulerPort = 10259
	// KubeControllerManagerPort 是控制管理器状态服务的默认端口。
	// 可在启动时通过参数覆盖。
	KubeControllerManagerPort = 10257
	// KubeAPIServerPort 是 API 服务器的默认端口。
	// 可在启动时通过参数覆盖。
	KubeAPIServerPort = 6443

	// EtcdAdvertiseClientUrlsAnnotationKey 是每个 etcd Pod 上描述通告客户端 URL 的注解键。
	EtcdAdvertiseClientUrlsAnnotationKey = "kubeadm.kubernetes.io/etcd.advertise-client-urls"
	// KubeAPIServerAdvertiseAddressEndpointAnnotationKey 是每个 API 服务器 Pod 上描述
	// API 端点（通告地址和 API 服务器监听端口）的注解键。
	KubeAPIServerAdvertiseAddressEndpointAnnotationKey = "kubeadm.kubernetes.io/kube-apiserver.advertise-address.endpoint"
	// ComponentConfigHashAnnotationKey 是 kubeadm 用于保存 SHA256 校验和以检测用户修改的 ConfigMap 注解键。
	ComponentConfigHashAnnotationKey = "kubeadm.kubernetes.io/component-config.hash"

	// ControlPlaneTier 是 tier 标签中用于标识控制平面组件的值。
	ControlPlaneTier = "control-plane"

	// Mode* 常量复制自 pkg/kubeapiserver/authorizer/modes，
	// 以避免 kubeadm 依赖 internal 模块。
	// TODO：在组件配置中共享 Mode* 常量。

	// ModeAlwaysAllow 将所有请求设为已授权的模式。
	ModeAlwaysAllow string = "AlwaysAllow"
	// ModeAlwaysDeny 将所有请求设为未授权的模式。
	ModeAlwaysDeny string = "AlwaysDeny"
	// ModeABAC 是使用基于属性的访问控制进行授权的模式。
	ModeABAC string = "ABAC"
	// ModeWebhook 是通过调用外部 Webhook 进行授权的模式。
	ModeWebhook string = "Webhook"
	// ModeRBAC 是使用基于角色的访问控制进行授权的模式。
	ModeRBAC string = "RBAC"
	// ModeNode 是授权 kubelet 发出的 API 请求的授权模式。
	ModeNode string = "Node"

	// PauseVersion 表示 kubeadm 使用的默认 pause 镜像版本。
	PauseVersion = "3.10.2"

	// CgroupDriverSystemd 保存 systemd 驱动类型。
	CgroupDriverSystemd = "systemd"

	// KubeControllerManagerUserName 是 kube-controller-manager 运行所用用户的用户名。
	KubeControllerManagerUserName string = "kubeadm-kcm"
	// KubeAPIServerUserName 是 kube-apiserver 运行所用用户的用户名。
	KubeAPIServerUserName string = "kubeadm-kas"
	// KubeSchedulerUserName 是 kube-scheduler 运行所用用户的用户名。
	KubeSchedulerUserName string = "kubeadm-ks"
	// EtcdUserName 是 etcd 运行所用用户的用户名。
	EtcdUserName string = "kubeadm-etcd"
	// ServiceAccountKeyReadersGroupName 是允许读取服务账号私钥的用户组。
	ServiceAccountKeyReadersGroupName string = "kubeadm-sa-key-readers"
	// UpgradeConfigurationKind 是 UpgradeConfiguration 结构体的字符串 kind 值。
	UpgradeConfigurationKind = "UpgradeConfiguration"

	// EnvVarInitDryRunDir 保存用于覆盖 init 试运行目录的环境变量。
	EnvVarInitDryRunDir = "KUBEADM_INIT_DRYRUN_DIR"
	// EnvVarJoinDryRunDir 保存用于覆盖 join 试运行目录的环境变量。
	EnvVarJoinDryRunDir = "KUBEADM_JOIN_DRYRUN_DIR"
	// EnvVarUpgradeDryRunDir 保存用于覆盖 upgrade 试运行目录的环境变量。
	EnvVarUpgradeDryRunDir = "KUBEADM_UPGRADE_DRYRUN_DIR"

	// ProbePort 是 Pod 清单中通用的命名端口。
	ProbePort = "probe-port"
)

var (
	// ControlPlaneTaint 是应用于 PodSpec 的污点，使该 Pod 能够调度到控制平面节点。
	ControlPlaneTaint = v1.Taint{
		Key:    LabelNodeRoleControlPlane,
		Effect: v1.TaintEffectNoSchedule,
	}

	// ControlPlaneComponents 定义控制平面组件名称。
	ControlPlaneComponents = []string{KubeAPIServer, KubeControllerManager, KubeScheduler}

	// MinimumControlPlaneVersion 指定 kubeadm 可部署的最低控制平面版本。
	MinimumControlPlaneVersion = getSkewedKubernetesVersion(-1)

	// MinimumKubeletVersion 指定 kubeadm 支持的最低 kubelet 版本。
	MinimumKubeletVersion = getSkewedKubernetesVersion(-3)

	// CurrentKubernetesVersion 指定 kubeadm 当前支持的 Kubernetes 版本。
	CurrentKubernetesVersion = getSkewedKubernetesVersion(0)

	// SupportedEtcdVersion 列出各 Kubernetes 版本官方支持的 etcd 版本。
	//
	// 更新此映射中的版本时，请同时更新：
	// - MinExternalEtcdVersion：取此映射中的最低 etcd 版本。
	// - DefaultEtcdVersion：取当前 k8s 版本使用的 etcd 版本。
	//
	// 映射长度最多为 3。kubeadm 支持相对控制平面版本最多偏差 -1，
	// 但发布前映射中最旧的 k8s 版本是唯一稳定版本，因此需要额外保留一个版本用于单元测试。
	//
	// 不要在 kubeadm 代码中直接使用此映射，应配合 EtcdSupportedVersion()。
	// 即使输入的 k8s 版本键未定义（超出范围），该函数也能返回有效版本。
	// 这样即使发布前尚未更新映射，kubeadm 也能确定 etcd 版本；此时用户会收到警告，
	// 因此最好每个版本都更新此映射。
	SupportedEtcdVersion = map[uint8]string{
		34: "3.7.0-0",
		35: "3.7.0-0",
		36: "3.7.0-0",
	}

	// KubeadmCertsClusterRoleName 设置允许引导令牌在新控制平面节点加入期间
	// 访问 kubeadm-certs Secret 的 ClusterRole 名称。
	KubeadmCertsClusterRoleName = fmt.Sprintf("kubeadm:%s", KubeadmCertsSecret)

	// DefaultKubernetesPlaceholderVersion 是构建时未填充 component-base 版本时使用的占位版本。
	DefaultKubernetesPlaceholderVersion = version.MustParseSemantic("v1.0.0-placeholder-version")
)

// getSkewedKubernetesVersion 返回当前 MAJOR.(MINOR+n).0 Kubernetes 版本，版本偏差为 n。
// 版本信息来自 component-base/version 包，必须在构建 kubeadm 时通过链接参数填充。
// 如果版本为空，则假定 kubeadm 构建有误，或代码正在单元测试中运行。
func getSkewedKubernetesVersion(n int) *version.Version {
	versionInfo := componentversion.Get()
	return getSkewedKubernetesVersionImpl(&versionInfo, n)
}

func getSkewedKubernetesVersionImpl(versionInfo *apimachineryversion.Info, n int) *version.Version {
	// TODO：如果 kubeadm 版本与 Kubernetes 版本解耦，需要更新此处。
	// 这需要用表格记录支持的版本偏差。
	// 如果 kubelet 版本将来也与 Kubernetes 版本解耦，还需要更多修改。
	var ver *version.Version
	if len(versionInfo.Major) == 0 {
		return DefaultKubernetesPlaceholderVersion
	}
	ver = version.MustParseSemantic(versionInfo.GitVersion)
	// 追加 MINOR 版本偏差。
	// TODO：处理 Kubernetes 将来迁移到 v2.0 或发生 MAJOR 版本升级的情况。
	// 这需要用表格记录特定 MAJOR 版本对应的最后一个 MINOR。
	minor := uint(int(ver.Minor()) + n)
	return version.MustParseSemantic(fmt.Sprintf("v%d.%d.0", ver.Major(), minor))
}

// EtcdSupportedVersion 返回指定 Kubernetes 版本官方支持的 etcd 版本。
// 如果传入版本不在给定列表中，则返回最接近的版本并发出警告。
func EtcdSupportedVersion(supportedEtcdVersion map[uint8]string, versionString string) (etcdVersion *version.Version, warning, err error) {
	kubernetesVersion, err := version.ParseSemantic(versionString)
	if err != nil {
		return nil, nil, err
	}
	desiredVersion, etcdStringVersion := uint8(kubernetesVersion.Minor()), ""

	min, max := ^uint8(0), uint8(0)
	for k, v := range supportedEtcdVersion {
		if desiredVersion == k {
			etcdStringVersion = v
			break
		}
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
	}

	if len(etcdStringVersion) == 0 {
		if desiredVersion < min {
			etcdStringVersion = supportedEtcdVersion[min]
		}
		if desiredVersion > max {
			etcdStringVersion = supportedEtcdVersion[max]
		}
		warning = errors.Errorf("could not find officially supported version of etcd for Kubernetes %s, falling back to the nearest etcd version (%s)",
			versionString, etcdStringVersion)
	}

	etcdVersion, err = version.ParseSemantic(etcdStringVersion)
	if err != nil {
		return nil, nil, err
	}

	return etcdVersion, warning, nil
}

// GetStaticPodDirectory 返回静态 Pod 在磁盘上的存放目录。
func GetStaticPodDirectory() string {
	return filepath.Join(KubernetesDir, ManifestsSubDirName)
}

// GetStaticPodFilepath 返回静态 Pod 在磁盘上的文件路径。
func GetStaticPodFilepath(componentName, manifestsDir string) string {
	return filepath.Join(manifestsDir, componentName+".yaml")
}

// GetAdminKubeConfigPath 返回管理员 kubeconfig 在磁盘上的默认位置。
func GetAdminKubeConfigPath() string {
	return filepath.Join(KubernetesDir, AdminKubeConfigFileName)
}

// GetKubeletKubeConfigPath 返回 kubelet kubeconfig 在磁盘上的默认位置。
func GetKubeletKubeConfigPath() string {
	return filepath.Join(KubernetesDir, KubeletKubeConfigFileName)
}

// GetDryRunDir 在 /etc/kubernetes/tmp 下创建临时目录。
// 如果设置了 envVar 指定的环境变量，则使用其值。
// msgFunc 用于向用户输出可通过 envVar 覆盖目录的提示。
func GetDryRunDir(envVar, dirName string, msgFunc func(format string, args ...interface{})) (string, error) {
	envVarDir := os.Getenv(envVar)
	if len(envVarDir) > 0 {
		return envVarDir, nil
	}
	tempDir := filepath.Join(KubernetesDir, TempDir)
	generatedDir, err := createTmpDir(tempDir, dirName)
	if err != nil {
		return "", err
	}

	msgFunc("Using dry-run directory %s. To override it, set the environment variable %s",
		generatedDir, envVar)

	return generatedDir, nil
}

// CreateTempDir 在 /etc/kubernetes/tmp 下创建临时目录。
// 如果指定了父目录，则在父目录的 tmp 子目录下创建。
func CreateTempDir(parent, dirName string) (string, error) {
	tempDir := filepath.Join(KubernetesDir, TempDir)
	if len(parent) > 0 {
		tempDir = filepath.Join(parent, TempDir)
	}
	return createTmpDir(tempDir, dirName)
}

func createTmpDir(tempDir, dirName string) (string, error) {
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		return "", errors.Wrapf(err, "failed to create directory %q", tempDir)
	}
	tempDir, err := os.MkdirTemp(tempDir, dirName)
	if err != nil {
		return "", errors.Wrapf(err, "could not create a temporary directory in %q", tempDir)
	}
	return tempDir, nil
}

// CreateTimestampDir 在 /etc/kubernetes/tmp 下创建以当前日期时间命名的临时目录。
func CreateTimestampDir(kubernetesDir, dirName string) (string, error) {
	tempDir := filepath.Join(KubernetesDir, TempDir)
	if len(kubernetesDir) != 0 {
		tempDir = filepath.Join(kubernetesDir, TempDir)
	}

	// 目标目录不存在时创建它。
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		return "", errors.Wrapf(err, "failed to create directory %q", tempDir)
	}

	timestampDirName := fmt.Sprintf("%s-%s", dirName, time.Now().Format("2006-01-02-15-04-05"))
	timestampDir := filepath.Join(tempDir, timestampDirName)
	if err := os.Mkdir(timestampDir, 0700); err != nil {
		return "", errors.Wrap(err, "could not create timestamp directory")
	}

	return timestampDir, nil
}

// GetDNSIP 返回 svcSubnet CIDR 范围内第十个 IP 作为 dnsIP。
func GetDNSIP(svcSubnetList string) (net.IP, error) {
	// 获取 Service 子网 CIDR。
	svcSubnetCIDR, err := GetKubernetesServiceCIDR(svcSubnetList)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get internal Kubernetes Service IP from the given service CIDR (%s)", svcSubnetList)
	}

	// 选择 Service 子网 CIDR 范围内第十个 IP 作为 dnsIP。
	dnsIP, err := netutils.GetIndexedIP(svcSubnetCIDR, 10)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get internal Kubernetes Service IP from the given service CIDR")
	}

	return dnsIP, nil
}

// GetKubernetesServiceCIDR 返回 Kubernetes 内部服务的默认 Service CIDR。
func GetKubernetesServiceCIDR(svcSubnetList string) (*net.IPNet, error) {
	// 集群默认 Service 地址族取自 kube-controller-manager 和 kube-apiserver 通过
	// `--service-cluster-ip-range` 参数配置的第一个 Service 集群 IP 范围的地址族。
	svcSubnets, err := netutils.ParseCIDRs(strings.Split(svcSubnetList, ","))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse ServiceSubnet %v", svcSubnetList)
	}
	if len(svcSubnets) == 0 {
		return nil, errors.New("received empty ServiceSubnet")
	}
	return svcSubnets[0], nil
}

// GetAPIServerVirtualIP 返回 Kubernetes 内部 API 服务的 IP。
func GetAPIServerVirtualIP(svcSubnetList string) (net.IP, error) {
	svcSubnet, err := GetKubernetesServiceCIDR(svcSubnetList)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get internal Kubernetes Service IP from the given service CIDR")
	}
	internalAPIServerVirtualIP, err := netutils.GetIndexedIP(svcSubnet, 1)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get the first IP address from the given CIDR: %s", svcSubnet.String())
	}
	return internalAPIServerVirtualIP, nil
}
