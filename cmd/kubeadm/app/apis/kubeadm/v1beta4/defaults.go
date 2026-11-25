/*
Copyright 2023 The Kubernetes Authors.

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

package v1beta4

import (
	"net/url"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
)

const (
	// DefaultServiceDNSDomain 定义服务和 Pod 的默认集群内部域名。
	DefaultServiceDNSDomain = "cluster.local"
	// DefaultServicesSubnet 定义默认服务子网范围。
	DefaultServicesSubnet = "10.96.0.0/12"
	// DefaultClusterDNSIP 定义默认 DNS IP。
	DefaultClusterDNSIP = "10.96.0.10"
	// DefaultKubernetesVersion 定义默认 Kubernetes 版本。
	DefaultKubernetesVersion = "stable-1"
	// DefaultCertificatesDir 定义默认证书目录。
	DefaultCertificatesDir = "/etc/kubernetes/pki"
	// DefaultImageRepository 定义默认镜像仓库（此前默认为 k8s.gcr.io）。
	DefaultImageRepository = "registry.k8s.io"
	// DefaultManifestsDir 定义默认清单目录。
	DefaultManifestsDir = "/etc/kubernetes/manifests"
	// DefaultClusterName 定义默认集群名称。
	DefaultClusterName = "kubernetes"

	// DefaultEtcdDataDir 定义静态 Pod 中 etcd 保存数据的默认目录。
	DefaultEtcdDataDir = "/var/lib/etcd"
	// DefaultProxyBindAddressv4 是公布地址为 IPv4 时的默认绑定地址。
	DefaultProxyBindAddressv4 = "0.0.0.0"
	// DefaultProxyBindAddressv6 是公布地址为 IPv6 时的默认绑定地址。
	DefaultProxyBindAddressv6 = "::"

	// DefaultImagePullPolicy 是 kubeadm 的默认镜像拉取策略。
	DefaultImagePullPolicy = corev1.PullIfNotPresent

	// DefaultEncryptionAlgorithm 是默认加密算法。
	DefaultEncryptionAlgorithm = EncryptionAlgorithmRSA2048
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_InitConfiguration 为 InitConfiguration 设置默认值。
func SetDefaults_InitConfiguration(obj *InitConfiguration) {
	SetDefaults_BootstrapTokens(obj)
	SetDefaults_APIEndpoint(&obj.LocalAPIEndpoint)
	SetDefaults_NodeRegistration(&obj.NodeRegistration)
	if obj.Timeouts == nil {
		obj.Timeouts = &Timeouts{}
	}
	SetDefaults_Timeouts(obj.Timeouts)
}

// SetDefaults_ClusterConfiguration 为 ClusterConfiguration 设置默认值。
func SetDefaults_ClusterConfiguration(obj *ClusterConfiguration) {
	if obj.KubernetesVersion == "" {
		obj.KubernetesVersion = DefaultKubernetesVersion
	}

	if obj.Networking.ServiceSubnet == "" {
		obj.Networking.ServiceSubnet = DefaultServicesSubnet
	}

	if obj.Networking.DNSDomain == "" {
		obj.Networking.DNSDomain = DefaultServiceDNSDomain
	}

	if obj.CertificatesDir == "" {
		obj.CertificatesDir = DefaultCertificatesDir
	}

	if obj.ImageRepository == "" {
		obj.ImageRepository = DefaultImageRepository
	}

	if obj.ClusterName == "" {
		obj.ClusterName = DefaultClusterName
	}

	if obj.EncryptionAlgorithm == "" {
		obj.EncryptionAlgorithm = DefaultEncryptionAlgorithm
	}

	if obj.CertificateValidityPeriod == nil {
		obj.CertificateValidityPeriod = &metav1.Duration{
			Duration: constants.CertificateValidityPeriod,
		}
	}
	if obj.CACertificateValidityPeriod == nil {
		obj.CACertificateValidityPeriod = &metav1.Duration{
			Duration: constants.CACertificateValidityPeriod,
		}
	}

	SetDefaults_Etcd(obj)
}

// SetDefaults_Etcd 为 etcd 配置设置默认值。
func SetDefaults_Etcd(obj *ClusterConfiguration) {
	if obj.Etcd.External == nil && obj.Etcd.Local == nil {
		obj.Etcd.Local = &LocalEtcd{}
	}
	if obj.Etcd.Local != nil {
		if obj.Etcd.Local.DataDir == "" {
			obj.Etcd.Local.DataDir = DefaultEtcdDataDir
		}
	}
	if obj.Etcd.External != nil {
		SetDefaults_ExternalEtcd(obj.Etcd.External)
	}
}

// SetDefaults_ExternalEtcd 为外部 etcd 配置设置默认值。
func SetDefaults_ExternalEtcd(obj *ExternalEtcd) {
	// 未设置 HTTPEndpoints 时使用 Endpoints，使 HTTP 流量（指标、健康检查）与 gRPC 流量共用端点。
	if len(obj.HTTPEndpoints) == 0 && len(obj.Endpoints) > 0 {
		obj.HTTPEndpoints = make([]string, len(obj.Endpoints))
		copy(obj.HTTPEndpoints, obj.Endpoints)
	}
}

// SetDefaults_JoinConfiguration 为普通加入节点设置默认值。
func SetDefaults_JoinConfiguration(obj *JoinConfiguration) {
	if obj.CACertPath == "" {
		obj.CACertPath = DefaultCACertPath
	}

	SetDefaults_JoinControlPlane(obj.ControlPlane)
	SetDefaults_Discovery(&obj.Discovery)
	SetDefaults_NodeRegistration(&obj.NodeRegistration)
	if obj.Timeouts == nil {
		obj.Timeouts = &Timeouts{}
	}
	SetDefaults_Timeouts(obj.Timeouts)
}

// SetDefaults_JoinControlPlane 为加入集群的控制平面节点设置默认值。
func SetDefaults_JoinControlPlane(obj *JoinControlPlane) {
	if obj != nil {
		SetDefaults_APIEndpoint(&obj.LocalAPIEndpoint)
	}
}

// SetDefaults_Discovery 为发现过程设置默认值。
func SetDefaults_Discovery(obj *Discovery) {
	if len(obj.TLSBootstrapToken) == 0 && obj.BootstrapToken != nil {
		obj.TLSBootstrapToken = obj.BootstrapToken.Token
	}

	if obj.File != nil {
		SetDefaults_FileDiscovery(obj.File)
	}
}

// SetDefaults_FileDiscovery 为基于文件的发现设置默认值。
func SetDefaults_FileDiscovery(obj *FileDiscovery) {
	// 确保 file URL 转换为路径。
	if len(obj.KubeConfigPath) != 0 {
		u, err := url.Parse(obj.KubeConfigPath)
		if err == nil && u.Scheme == "file" {
			obj.KubeConfigPath = u.Path
		}
	}
}

// SetDefaults_BootstrapTokens 为 .BootstrapTokens 字段设置默认值。
// 切片为空时默认填入一个令牌；否则遍历切片，为 TTL、Usages 和 Groups 等可省略字段设置默认值。
// API 默认化层不会为 Token 生成随机值；若仍未设置，将在运行时补充随机值。
func SetDefaults_BootstrapTokens(obj *InitConfiguration) {

	if len(obj.BootstrapTokens) == 0 {
		obj.BootstrapTokens = []bootstraptokenv1.BootstrapToken{{}}
	}

	for i := range obj.BootstrapTokens {
		bootstraptokenv1.SetDefaults_BootstrapToken(&obj.BootstrapTokens[i])
	}
}

// SetDefaults_APIEndpoint 为部署在节点上的 API Server 实例设置默认值。
func SetDefaults_APIEndpoint(obj *APIEndpoint) {
	if obj.BindPort == 0 {
		obj.BindPort = constants.KubeAPIServerPort
	}
}

// SetDefaults_NodeRegistration 为 NodeRegistrationOptions 对象设置默认值。
func SetDefaults_NodeRegistration(obj *NodeRegistrationOptions) {
	if len(obj.ImagePullPolicy) == 0 {
		obj.ImagePullPolicy = DefaultImagePullPolicy
	}
	if obj.ImagePullSerial == nil {
		obj.ImagePullSerial = ptr.To(true)
	}
}

// SetDefaults_ResetConfiguration 为 ResetConfiguration 对象设置默认值。
func SetDefaults_ResetConfiguration(obj *ResetConfiguration) {
	if obj.CertificatesDir == "" {
		obj.CertificatesDir = DefaultCertificatesDir
	}
	if obj.Timeouts == nil {
		obj.Timeouts = &Timeouts{}
	}
	SetDefaults_Timeouts(obj.Timeouts)
}

// SetDefaults_EnvVar 为 EnvVar 设置默认值。
// +k8s:defaulter-gen=covers
func SetDefaults_EnvVar(obj *EnvVar) {
	if obj.ValueFrom != nil {
		if obj.ValueFrom.FieldRef != nil {
			if obj.ValueFrom.FieldRef.APIVersion == "" {
				obj.ValueFrom.FieldRef.APIVersion = "v1"
			}
		}
	}
}

// SetDefaults_Timeouts 为超时设置默认值。
func SetDefaults_Timeouts(obj *Timeouts) {
	if obj.ControlPlaneComponentHealthCheck == nil {
		obj.ControlPlaneComponentHealthCheck = &metav1.Duration{
			Duration: constants.ControlPlaneComponentHealthCheckTimeout,
		}
	}
	if obj.KubeletHealthCheck == nil {
		obj.KubeletHealthCheck = &metav1.Duration{
			Duration: constants.KubeletHealthCheckTimeout,
		}
	}
	if obj.KubernetesAPICall == nil {
		obj.KubernetesAPICall = &metav1.Duration{
			Duration: constants.KubernetesAPICallTimeout,
		}
	}
	if obj.EtcdAPICall == nil {
		obj.EtcdAPICall = &metav1.Duration{
			Duration: constants.EtcdAPICallTimeout,
		}
	}
	if obj.TLSBootstrap == nil {
		obj.TLSBootstrap = &metav1.Duration{
			Duration: constants.TLSBootstrapTimeout,
		}
	}
	if obj.Discovery == nil {
		obj.Discovery = &metav1.Duration{
			Duration: constants.DiscoveryTimeout,
		}
	}
	if obj.UpgradeManifests == nil {
		obj.UpgradeManifests = &metav1.Duration{
			Duration: constants.UpgradeManifestsTimeout,
		}
	}
}

// SetDefaults_UpgradeConfiguration 为 UpgradeConfiguration 设置默认值。
func SetDefaults_UpgradeConfiguration(obj *UpgradeConfiguration) {
	if obj.Node.EtcdUpgrade == nil {
		obj.Node.EtcdUpgrade = ptr.To(true)
	}
	if obj.Node.CertificateRenewal == nil {
		obj.Node.CertificateRenewal = ptr.To(true)
	}
	if len(obj.Node.ImagePullPolicy) == 0 {
		obj.Node.ImagePullPolicy = DefaultImagePullPolicy
	}
	if obj.Node.ImagePullSerial == nil {
		obj.Node.ImagePullSerial = ptr.To(true)
	}

	if obj.Apply.EtcdUpgrade == nil {
		obj.Apply.EtcdUpgrade = ptr.To(true)
	}
	if obj.Apply.CertificateRenewal == nil {
		obj.Apply.CertificateRenewal = ptr.To(true)
	}
	if len(obj.Apply.ImagePullPolicy) == 0 {
		obj.Apply.ImagePullPolicy = DefaultImagePullPolicy
	}
	if obj.Apply.ImagePullSerial == nil {
		obj.Apply.ImagePullSerial = ptr.To(true)
	}

	if obj.Plan.EtcdUpgrade == nil {
		obj.Plan.EtcdUpgrade = ptr.To(true)
	}

	if obj.Timeouts == nil {
		obj.Timeouts = &Timeouts{}
	}
	SetDefaults_Timeouts(obj.Timeouts)
}
