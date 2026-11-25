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

package componentconfigs

import (
	"path/filepath"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"
	"k8s.io/utils/ptr"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// KubeletGroup 指向 kubelet 配置使用的 API 组名。
	KubeletGroup = kubeletconfig.GroupName

	// kubeletReadOnlyPort 指定不安全 HTTP 服务的默认端口。
	// 值为 0 时禁用不安全 HTTP 服务。
	kubeletReadOnlyPort int32 = 0

	// kubeletRotateCertificates 指定启用证书轮换的默认值。
	kubeletRotateCertificates = true

	// kubeletAuthenticationAnonymousEnabled 指定禁用匿名访问的默认值。
	kubeletAuthenticationAnonymousEnabled = false

	// kubeletAuthenticationWebhookEnabled 指定启用认证 Webhook 的默认值。
	kubeletAuthenticationWebhookEnabled = true

	// kubeletHealthzBindAddress 指定 healthz 服务的默认监听地址。
	kubeletHealthzBindAddress = "127.0.0.1"

	// kubeletSystemdResolverConfig 指定 systemd 服务处于活动状态时使用的默认解析器配置。
	kubeletSystemdResolverConfig = "/run/systemd/resolve/resolv.conf"
)

// kubeletHandler 是 kubelet 组件配置的处理实例。
var kubeletHandler = handler{
	GroupVersion: kubeletconfig.SchemeGroupVersion,
	AddToScheme:  kubeletconfig.AddToScheme,
	CreateEmpty: func() kubeadmapi.ComponentConfig {
		return &kubeletConfig{
			configBase: configBase{
				GroupVersion: kubeletconfig.SchemeGroupVersion,
			},
		}
	},
	fromCluster: kubeletConfigFromCluster,
}

func kubeletConfigFromCluster(h *handler, clientset clientset.Interface, _ *kubeadmapi.ClusterConfiguration) (kubeadmapi.ComponentConfig, error) {
	configMapName := constants.KubeletBaseConfigurationConfigMap
	klog.V(1).Infof("attempting to download the KubeletConfiguration from ConfigMap %q", configMapName)
	cm, err := h.fromConfigMap(clientset, configMapName, constants.KubeletBaseConfigurationConfigMapKey, true)
	if err != nil {
		return nil, errors.Wrapf(err, "could not download the kubelet configuration from ConfigMap %q",
			configMapName)
	}
	return cm, nil
}

// kubeletConfig 为 kubelet 实现 kubeadmapi.ComponentConfig 接口。
type kubeletConfig struct {
	configBase
	config kubeletconfig.KubeletConfiguration
}

// DeepCopy 深拷贝配置，避免调用方共享内部 map 和 slice。
func (kc *kubeletConfig) DeepCopy() kubeadmapi.ComponentConfig {
	result := &kubeletConfig{}
	kc.configBase.DeepCopyInto(&result.configBase)
	kc.config.DeepCopyInto(&result.config)
	return result
}

// Marshal 使用配置所属 GroupVersion 将 kubelet 配置编码为 YAML。
func (kc *kubeletConfig) Marshal() ([]byte, error) {
	return kc.configBase.Marshal(&kc.config)
}

// Unmarshal 从文档集合解析 kubelet 配置；版本不匹配时要求调用方手动转换。
func (kc *kubeletConfig) Unmarshal(docmap kubeadmapi.DocumentMap) error {
	return kc.configBase.Unmarshal(docmap, &kc.config)
}

// Get 返回内部配置指针，调用方修改会直接反映到当前对象。
func (kc *kubeletConfig) Get() interface{} {
	return &kc.config
}

// Set 替换内部配置值；cfg 必须是 *kubeletconfig.KubeletConfiguration。
func (kc *kubeletConfig) Set(cfg interface{}) {
	kc.config = *cfg.(*kubeletconfig.KubeletConfiguration)
}

// Default 依据集群配置补齐 kubelet 默认值，同时保留用户显式设置的非默认路径。
func (kc *kubeletConfig) Default(cfg *kubeadmapi.ClusterConfiguration, _ *kubeadmapi.APIEndpoint, nodeRegOpts *kubeadmapi.NodeRegistrationOptions) {
	const kind = "KubeletConfiguration"

	if kc.config.FeatureGates == nil {
		kc.config.FeatureGates = map[string]bool{}
	}

	if kc.config.StaticPodPath == "" {
		kc.config.StaticPodPath = kubeadmapiv1.DefaultManifestsDir
	} else if kc.config.StaticPodPath != kubeadmapiv1.DefaultManifestsDir {
		warnDefaultComponentConfigValue(kind, "staticPodPath", kubeadmapiv1.DefaultManifestsDir, kc.config.StaticPodPath)
	}

	clusterDNS := ""
	dnsIP, err := constants.GetDNSIP(cfg.Networking.ServiceSubnet)
	if err != nil {
		clusterDNS = kubeadmapiv1.DefaultClusterDNSIP
	} else {
		clusterDNS = dnsIP.String()
	}

	if kc.config.ClusterDNS == nil {
		kc.config.ClusterDNS = []string{clusterDNS}
	} else if len(kc.config.ClusterDNS) != 1 || kc.config.ClusterDNS[0] != clusterDNS {
		warnDefaultComponentConfigValue(kind, "clusterDNS", []string{clusterDNS}, kc.config.ClusterDNS)
	}

	if kc.config.ClusterDomain == "" {
		kc.config.ClusterDomain = cfg.Networking.DNSDomain
	} else if cfg.Networking.DNSDomain != "" && kc.config.ClusterDomain != cfg.Networking.DNSDomain {
		warnDefaultComponentConfigValue(kind, "clusterDomain", cfg.Networking.DNSDomain, kc.config.ClusterDomain)
	}

	// 要求所有访问 kubelet API 的客户端提供由集群 CA 签发的客户端证书。
	clientCAFile := filepath.Join(cfg.CertificatesDir, constants.CACertName)
	if kc.config.Authentication.X509.ClientCAFile == "" {
		kc.config.Authentication.X509.ClientCAFile = clientCAFile
	} else if kc.config.Authentication.X509.ClientCAFile != clientCAFile {
		warnDefaultComponentConfigValue(kind, "authentication.x509.clientCAFile", clientCAFile, kc.config.Authentication.X509.ClientCAFile)
	}

	if kc.config.Authentication.Anonymous.Enabled == nil {
		kc.config.Authentication.Anonymous.Enabled = ptr.To(kubeletAuthenticationAnonymousEnabled)
	} else if *kc.config.Authentication.Anonymous.Enabled {
		warnDefaultComponentConfigValue(kind, "authentication.anonymous.enabled", kubeletAuthenticationAnonymousEnabled, *kc.config.Authentication.Anonymous.Enabled)
	}

	// 每次收到 kubelet API 客户端请求时，向 API 服务器执行 Webhook（SubjectAccessReview 请求），
	// 询问该客户端是否有权访问 kubelet API。
	if kc.config.Authorization.Mode == "" {
		kc.config.Authorization.Mode = kubeletconfig.KubeletAuthorizationModeWebhook
	} else if kc.config.Authorization.Mode != kubeletconfig.KubeletAuthorizationModeWebhook {
		warnDefaultComponentConfigValue(kind, "authorization.mode", kubeletconfig.KubeletAuthorizationModeWebhook, kc.config.Authorization.Mode)
	}

	// 允许使用 ServiceAccount 令牌等其他认证方式的客户端访问 kubelet API。
	if kc.config.Authentication.Webhook.Enabled == nil {
		kc.config.Authentication.Webhook.Enabled = ptr.To(kubeletAuthenticationWebhookEnabled)
	} else if !*kc.config.Authentication.Webhook.Enabled {
		warnDefaultComponentConfigValue(kind, "authentication.webhook.enabled", kubeletAuthenticationWebhookEnabled, *kc.config.Authentication.Webhook.Enabled)
	}

	// 在 127.0.0.1:10248 上提供 /healthz Web 服务，供 kubeadm 访问。
	if kc.config.HealthzBindAddress == "" {
		kc.config.HealthzBindAddress = kubeletHealthzBindAddress
	} else if kc.config.HealthzBindAddress != kubeletHealthzBindAddress {
		warnDefaultComponentConfigValue(kind, "healthzBindAddress", kubeletHealthzBindAddress, kc.config.HealthzBindAddress)
	}

	if kc.config.HealthzPort == nil {
		kc.config.HealthzPort = ptr.To[int32](constants.KubeletHealthzPort)
	} else if *kc.config.HealthzPort != constants.KubeletHealthzPort {
		warnDefaultComponentConfigValue(kind, "healthzPort", constants.KubeletHealthzPort, *kc.config.HealthzPort)
	}

	if kc.config.ReadOnlyPort != kubeletReadOnlyPort {
		warnDefaultComponentConfigValue(kind, "readOnlyPort", kubeletReadOnlyPort, kc.config.ReadOnlyPort)
	}

	// RotateCertificates 为非指针字段，无法判断用户是否设置过 false，因此不能警告并强制设为 true。
	kc.config.RotateCertificates = kubeletRotateCertificates

	if len(kc.config.CgroupDriver) == 0 {
		klog.V(1).Infof("the value of KubeletConfiguration.cgroupDriver is empty; setting it to %q", constants.CgroupDriverSystemd)
		kc.config.CgroupDriver = constants.CgroupDriverSystemd
	}
}
