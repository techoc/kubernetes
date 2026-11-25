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
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	kubeproxyconfig "k8s.io/kube-proxy/config/v1alpha1"
	netutils "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// KubeProxyGroup 指向 kube-proxy 配置使用的 API 组名。
	KubeProxyGroup = kubeproxyconfig.GroupName

	// kubeproxyKubeConfigFileName 用于默认化；放在这里供测试访问。
	kubeproxyKubeConfigFileName = "/var/lib/kube-proxy/kubeconfig.conf"
)

// kubeProxyHandler 是 kube-proxy 组件配置的处理实例。
var kubeProxyHandler = handler{
	GroupVersion: kubeproxyconfig.SchemeGroupVersion,
	AddToScheme:  kubeproxyconfig.AddToScheme,
	CreateEmpty: func() kubeadmapi.ComponentConfig {
		return &kubeProxyConfig{
			configBase: configBase{
				GroupVersion: kubeproxyconfig.SchemeGroupVersion,
			},
		}
	},
	fromCluster: kubeProxyConfigFromCluster,
}

func kubeProxyConfigFromCluster(h *handler, clientset clientset.Interface, _ *kubeadmapi.ClusterConfiguration) (kubeadmapi.ComponentConfig, error) {
	configMapName := kubeadmconstants.KubeProxyConfigMap
	klog.V(1).Infof("attempting to download the KubeProxyConfiguration from ConfigMap %q", configMapName)
	cm, err := h.fromConfigMap(clientset, configMapName, kubeadmconstants.KubeProxyConfigMapKey, false)
	if err != nil {
		return nil, errors.Wrapf(err, "could not download the kube-proxy configuration from ConfigMap %q",
			configMapName)
	}
	return cm, nil
}

// kubeProxyConfig 为 kube-proxy 实现 kubeadmapi.ComponentConfig 接口。
type kubeProxyConfig struct {
	configBase
	config kubeproxyconfig.KubeProxyConfiguration
}

// DeepCopy 深拷贝配置，避免调用方共享内部 map 和 slice。
func (kp *kubeProxyConfig) DeepCopy() kubeadmapi.ComponentConfig {
	result := &kubeProxyConfig{}
	kp.configBase.DeepCopyInto(&result.configBase)
	kp.config.DeepCopyInto(&result.config)
	return result
}

// Marshal 使用配置所属 GroupVersion 将 kube-proxy 配置编码为 YAML。
func (kp *kubeProxyConfig) Marshal() ([]byte, error) {
	return kp.configBase.Marshal(&kp.config)
}

// Unmarshal 从文档集合解析 kube-proxy 配置；版本不匹配时要求调用方手动转换。
func (kp *kubeProxyConfig) Unmarshal(docmap kubeadmapi.DocumentMap) error {
	return kp.configBase.Unmarshal(docmap, &kp.config)
}

func kubeProxyDefaultBindAddress(address string) string {
	if ip := netutils.ParseIPSloppy(address); ip != nil && ip.To4() != nil {
		return kubeadmapiv1.DefaultProxyBindAddressv4
	}
	return kubeadmapiv1.DefaultProxyBindAddressv6
}

// isWildcardBindAddress 判断给定 bindAddress 是否为推荐的通配默认地址（IPv4 为 0.0.0.0，IPv6 为 ::）。
func isWildcardBindAddress(bindAddress string) bool {
	return bindAddress == kubeadmapiv1.DefaultProxyBindAddressv4 ||
		bindAddress == kubeadmapiv1.DefaultProxyBindAddressv6
}

// Get 返回内部配置指针，调用方修改会直接反映到当前对象。
func (kp *kubeProxyConfig) Get() interface{} {
	return &kp.config
}

// Set 替换内部配置值；cfg 必须是 *kubeproxyconfig.KubeProxyConfiguration。
func (kp *kubeProxyConfig) Set(cfg interface{}) {
	kp.config = *cfg.(*kubeproxyconfig.KubeProxyConfiguration)
}

// Default 依据集群和本地 API 端点补齐 kube-proxy 默认值；非法 bindAddress 会保留并发出警告。
func (kp *kubeProxyConfig) Default(cfg *kubeadmapi.ClusterConfiguration, localAPIEndpoint *kubeadmapi.APIEndpoint, _ *kubeadmapi.NodeRegistrationOptions) {
	const kind = "KubeProxyConfiguration"

	// 即使定义了 KubeProxy，用户也可能未定义任何 feature-gates。
	// 此时 FeatureGates 为 nil，后续插入 feature-gates 会导致 panic，因此需要先初始化。
	if kp.config.FeatureGates == nil {
		kp.config.FeatureGates = map[string]bool{}
	}

	switch {
	case kp.config.BindAddress == "":
		kp.config.BindAddress = kubeProxyDefaultBindAddress(localAPIEndpoint.AdvertiseAddress)
	case isWildcardBindAddress(kp.config.BindAddress):
		// 0.0.0.0 和 :: 都是有效的显式通配绑定地址。
	case netutils.ParseIPSloppy(kp.config.BindAddress) == nil:
		klog.Warningf("The bindAddress %q in %q is not a valid IP address", kp.config.BindAddress, kind)
	default:
		// bindAddress 不是其 IP 协议族推荐的通配默认地址时发出警告。
		// IPv4 推荐 0.0.0.0，IPv6 推荐 ::。
		warnDefaultComponentConfigValue(kind, "bindAddress",
			kubeProxyDefaultBindAddress(kp.config.BindAddress), kp.config.BindAddress)
	}

	if kp.config.ClusterCIDR == "" && cfg.Networking.PodSubnet != "" {
		kp.config.ClusterCIDR = cfg.Networking.PodSubnet
	} else if cfg.Networking.PodSubnet != "" && kp.config.ClusterCIDR != cfg.Networking.PodSubnet {
		warnDefaultComponentConfigValue(kind, "clusterCIDR", cfg.Networking.PodSubnet, kp.config.ClusterCIDR)
	}

	if kp.config.ClientConnection.Kubeconfig == "" {
		kp.config.ClientConnection.Kubeconfig = kubeproxyKubeConfigFileName
	} else if kp.config.ClientConnection.Kubeconfig != kubeproxyKubeConfigFileName {
		warnDefaultComponentConfigValue(kind, "clientConnection.kubeconfig", kubeproxyKubeConfigFileName, kp.config.ClientConnection.Kubeconfig)
	}

	if kp.config.Mode == "ipvs" {
		klog.Warningf("The %q field \"mode\" is set to \"ipvs\" which has been deprecated since version v1.35. "+
			"For newer Linux kernel versions, we recommend using the \"nftables\" mode, which has been GA since v1.33. "+
			"For older Linux kernel versions, you can use the \"iptables\" mode which is still the default one.", kind)
	}

	// 用户未显式设置 mode 时，kube-proxy 会发出警告。
	// 未来版本默认模式也会从 "iptables" 切换为 "nftables"。
	// TODO：未来版本需要完成以下必要修改：
	// - https://github.com/kubernetes/kubeadm/issues/3309
	if kp.config.Mode == "" {
		kp.config.Mode = "iptables"
	}
}

// Mutate 对 kube-proxy 配置不执行任何操作。
func (kp *kubeProxyConfig) Mutate() error {
	return nil
}
