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

package config

import (
	"bytes"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	netutil "k8s.io/apimachinery/pkg/util/net"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/config/strict"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeadmruntime "k8s.io/kubernetes/cmd/kubeadm/app/util/runtime"
)

var (
	// PlaceholderToken 仅静态设置，避免 kubeadm 每次运行都随机生成令牌。
	PlaceholderToken = bootstraptokenv1.BootstrapToken{
		Token: &bootstraptokenv1.BootstrapTokenString{
			ID:     "abcdef",
			Secret: "0123456789abcdef",
		},
	}
)

// SetInitDynamicDefaults 检查并设置 InitConfiguration 的配置值。
func SetInitDynamicDefaults(cfg *kubeadmapi.InitConfiguration, skipCRIDetect, skipAPIEndpoint bool) error {
	if err := SetBootstrapTokensDynamicDefaults(&cfg.BootstrapTokens); err != nil {
		return err
	}
	if err := SetNodeRegistrationDynamicDefaults(&cfg.NodeRegistration, true, skipCRIDetect); err != nil {
		return err
	}
	if !skipAPIEndpoint {
		if err := SetAPIEndpointDynamicDefaults(&cfg.LocalAPIEndpoint); err != nil {
			return err
		}
	}
	return SetClusterDynamicDefaults(&cfg.ClusterConfiguration, &cfg.LocalAPIEndpoint, &cfg.NodeRegistration)
}

// SetBootstrapTokensDynamicDefaults 检查并设置 BootstrapTokens 的配置值。
func SetBootstrapTokensDynamicDefaults(cfg *[]bootstraptokenv1.BootstrapToken) error {
	// .Token 未设置时填入随机值。此逻辑放在当前层而非 API 默认值层，
	// 既考虑安全因素，也因为 API 默认值过程无法返回错误，而此处可以。
	for i, bt := range *cfg {
		if bt.Token != nil && len(bt.Token.String()) > 0 {
			continue
		}

		tokenStr, err := bootstraputil.GenerateBootstrapToken()
		if err != nil {
			return errors.Wrap(err, "couldn't generate random token")
		}
		token, err := bootstraptokenv1.NewBootstrapTokenString(tokenStr)
		if err != nil {
			return err
		}
		(*cfg)[i].Token = token
	}

	return nil
}

// SetNodeRegistrationDynamicDefaults 检查并设置 NodeRegistration 的配置值。
func SetNodeRegistrationDynamicDefaults(cfg *kubeadmapi.NodeRegistrationOptions, controlPlaneTaint, skipCRIDetect bool) error {
	var err error
	cfg.Name, err = nodeutil.GetHostname(cfg.Name)
	if err != nil {
		return err
	}

	// 仅在切片为 nil 时追加控制平面污点，从而允许用户用空切片禁用默认污点。
	if controlPlaneTaint && cfg.Taints == nil {
		cfg.Taints = []v1.Taint{kubeadmconstants.ControlPlaneTaint}
	}

	if cfg.CRISocket == "" {
		if skipCRIDetect {
			klog.V(4).Infof("skip CRI socket detection, fill with the default CRI socket %s", kubeadmconstants.DefaultCRISocket)
			cfg.CRISocket = kubeadmconstants.DefaultCRISocket
			return nil
		}
		cfg.CRISocket, err = kubeadmruntime.DetectCRISocket()
		if err != nil {
			return err
		}
		klog.V(1).Infof("detected and using CRI socket: %s", cfg.CRISocket)
	} else {
		if !strings.HasPrefix(cfg.CRISocket, kubeadmapiv1.DefaultContainerRuntimeURLScheme) {
			klog.Warningf("Usage of CRI endpoints without URL scheme is deprecated and can cause kubelet errors "+
				"in the future. Automatically prepending scheme %q to the \"criSocket\" with value %q. "+
				"Please update your configuration!", kubeadmapiv1.DefaultContainerRuntimeURLScheme, cfg.CRISocket)
			cfg.CRISocket = kubeadmapiv1.DefaultContainerRuntimeURLScheme + "://" + cfg.CRISocket
		}
	}

	return nil
}

// SetAPIEndpointDynamicDefaults 检查并设置 APIEndpoint 的配置值。
func SetAPIEndpointDynamicDefaults(cfg *kubeadmapi.APIEndpoint) error {
	// 校验 cfg.API.AdvertiseAddress。
	addressIP := netutils.ParseIPSloppy(cfg.AdvertiseAddress)
	if addressIP == nil && cfg.AdvertiseAddress != "" {
		return errors.Errorf("couldn't use \"%s\" as \"apiserver-advertise-address\", must be ipv4 or ipv6 address", cfg.AdvertiseAddress)
	}

	// kubeadm 允许将 address=Loopback 作为选择器，查找回环接口上的全局单播地址。
	// 例如网络存在默认路由但接口仅使用链路本地地址时（如 RFC5549 所述），需要此方式。
	if addressIP.IsLoopback() {
		loopbackIP, err := netutil.ChooseBindAddressForInterface(netutil.LoopbackInterfaceName)
		if err != nil {
			return err
		}
		if loopbackIP != nil {
			klog.V(4).Infof("Found active IP %v on loopback interface", loopbackIP.String())
			cfg.AdvertiseAddress = loopbackIP.String()
			return nil
		}
		return errors.New("unable to resolve link-local addresses")
	}

	// 逻辑与 API Server 相同，但找不到接口时将地址设为 0.0.0.0；该地址无效，不能用于引导集群。
	ip, err := ChooseAPIServerBindAddress(addressIP)
	if err != nil {
		return err
	}
	cfg.AdvertiseAddress = ip.String()

	return nil
}

// SetClusterDynamicDefaults 检查并设置 ClusterConfiguration 的值。
func SetClusterDynamicDefaults(cfg *kubeadmapi.ClusterConfiguration, localAPIEndpoint *kubeadmapi.APIEndpoint, nodeRegOpts *kubeadmapi.NodeRegistrationOptions) error {
	// 为所有内嵌的 ComponentConfig 结构设置默认值。
	componentconfigs.Default(cfg, localAPIEndpoint, nodeRegOpts)

	// 解析可能的版本标签并校验版本字符串。
	if err := NormalizeKubernetesVersion(cfg); err != nil {
		return err
	}

	// ControlPlaneEndpoint 未指定端口时，默认使用 APIEndpoint 的 bindPort。
	// 这样可使用不同 bindPort 的额外控制平面实例加入集群。
	if cfg.ControlPlaneEndpoint != "" {
		host, port, err := kubeadmutil.ParseHostPort(cfg.ControlPlaneEndpoint)
		if err != nil {
			return err
		}
		if port == "" {
			cfg.ControlPlaneEndpoint = net.JoinHostPort(host, strconv.FormatInt(int64(localAPIEndpoint.BindPort), 10))
		}
	}

	// 将 SAN 转为小写；ELB 等域名可能包含大写字母。
	LowercaseSANs(cfg.APIServer.CertSANs)
	return nil
}

// DefaultedStaticInitConfiguration 返回已应用静态默认值的内部 InitConfiguration。
func DefaultedStaticInitConfiguration() (*kubeadmapi.InitConfiguration, error) {
	versionedInitCfg := &kubeadmapiv1.InitConfiguration{
		LocalAPIEndpoint: kubeadmapiv1.APIEndpoint{AdvertiseAddress: "1.2.3.4"},
		BootstrapTokens:  []bootstraptokenv1.BootstrapToken{PlaceholderToken},
		NodeRegistration: kubeadmapiv1.NodeRegistrationOptions{
			CRISocket: kubeadmconstants.DefaultCRISocket, // CRISocket: kubeadmconstants.DefaultCRISocket, // 避免 CRI 检测
			Name:      "node",
		},
	}
	versionedClusterCfg := &kubeadmapiv1.ClusterConfiguration{
		KubernetesVersion: kubeadmconstants.CurrentKubernetesVersion.String(), // KubernetesVersion: kubeadmconstants.CurrentKubernetesVersion.String(), // 避免联网获取当前 Kubernetes 版本
	}

	internalcfg := &kubeadmapi.InitConfiguration{}

	// 考虑已传入的参数；再次执行默认值设置，仅对参数未提供的 cfg 字段赋静态默认值。
	kubeadmscheme.Scheme.Default(versionedInitCfg)
	if err := kubeadmscheme.Scheme.Convert(versionedInitCfg, internalcfg, nil); err != nil {
		return nil, err
	}

	kubeadmscheme.Scheme.Default(versionedClusterCfg)
	if err := kubeadmscheme.Scheme.Convert(versionedClusterCfg, &internalcfg.ClusterConfiguration, nil); err != nil {
		return nil, err
	}

	// 为所有内嵌的 ComponentConfig 结构设置默认值。
	componentconfigs.Default(&internalcfg.ClusterConfiguration, &internalcfg.LocalAPIEndpoint, &internalcfg.NodeRegistration)

	return internalcfg, nil
}

// DefaultedInitConfiguration 接收通常由参数填充的版本化 init 配置，设置默认值并转换为内部 InitConfiguration。
func DefaultedInitConfiguration(versionedInitCfg *kubeadmapiv1.InitConfiguration, versionedClusterCfg *kubeadmapiv1.ClusterConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.InitConfiguration, error) {
	internalcfg := &kubeadmapi.InitConfiguration{}

	// 考虑已传入的参数；再次执行默认值设置，仅对参数未提供的 cfg 字段赋静态默认值。
	kubeadmscheme.Scheme.Default(versionedInitCfg)
	if err := kubeadmscheme.Scheme.Convert(versionedInitCfg, internalcfg, nil); err != nil {
		return nil, err
	}

	kubeadmscheme.Scheme.Default(versionedClusterCfg)
	if err := kubeadmscheme.Scheme.Convert(versionedClusterCfg, &internalcfg.ClusterConfiguration, nil); err != nil {
		return nil, err
	}

	// 对参数未提供的设置应用动态默认值。
	if err := SetInitDynamicDefaults(internalcfg, opts.SkipCRIDetect, false); err != nil {
		return nil, err
	}
	// 校验 cfg（参数/配置 + 默认值 + 动态默认值）。
	if err := validation.ValidateInitConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}
	return internalcfg, nil
}

// LoadInitConfigurationFromFile 从文件加载受支持的版本化 InitConfiguration，
// 转换为内部配置、设置默认值并验证。
func LoadInitConfigurationFromFile(cfgPath string, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.InitConfiguration, error) {
	klog.V(1).Infof("loading configuration from %q", cfgPath)

	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read config from %q ", cfgPath)
	}

	return BytesToInitConfiguration(b, opts.SkipCRIDetect)
}

// LoadOrDefaultInitConfiguration 接收配置文件路径和可作为默认值的版本化配置。
// 指定 cfgPath 时，文件中的版本化配置始终覆盖传入配置。随后将外部版本化配置设置默认值并转换
// 为内部类型，再应用本机 IP 等动态默认值，最后校验并返回内部配置。
func LoadOrDefaultInitConfiguration(cfgPath string, versionedInitCfg *kubeadmapiv1.InitConfiguration, versionedClusterCfg *kubeadmapiv1.ClusterConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.InitConfiguration, error) {
	var (
		config *kubeadmapi.InitConfiguration
		err    error
	)
	if cfgPath != "" {
		// 若提供了配置文件，从中加载配置。
		config, err = LoadInitConfigurationFromFile(cfgPath, opts)
	} else {
		config, err = DefaultedInitConfiguration(versionedInitCfg, versionedClusterCfg, opts)
	}
	if err == nil {
		prepareStaticVariables(config)
	}
	return config, err
}

// BytesToInitConfiguration 将字节切片转换为已设置默认值并校验的内部 InitConfiguration。
// 输入可包含多个 YAML/JSON 文档，逐个解析；已知 ComponentConfig GVK 存入内部
// InitConfiguration。返回前应用动态默认值并校验。
func BytesToInitConfiguration(b []byte, skipCRIDetect bool) (*kubeadmapi.InitConfiguration, error) {
	// 将文件中的 YAML/JSON 文档拆分为 DocumentMap。
	gvkmap, err := kubeadmutil.SplitConfigDocuments(b)
	if err != nil {
		return nil, err
	}

	return documentMapToInitConfiguration(gvkmap, false, false, false, skipCRIDetect)
}

// documentMapToInitConfiguration 将 GVK 到 YAML/JSON 文档的映射转换为已默认化并校验的配置。
func documentMapToInitConfiguration(gvkmap kubeadmapi.DocumentMap, allowDeprecated, allowExperimental, strictErrors, skipCRIDetect bool) (*kubeadmapi.InitConfiguration, error) {
	var initcfg *kubeadmapi.InitConfiguration
	var clustercfg *kubeadmapi.ClusterConfiguration

	// 按 GVK 字符串确定性排序，使 ClusterConfiguration 优先解码。
	gvks := make([]schema.GroupVersionKind, 0, len(gvkmap))
	for gvk := range gvkmap {
		gvks = append(gvks, gvk)
	}
	sort.Slice(gvks, func(i, j int) bool {
		return gvks[i].String() < gvks[j].String()
	})

	for _, gvk := range gvks {
		fileContent := gvkmap[gvk]

		// 首先检查 GVK 是否受支持且未弃用。
		if err := validateSupportedVersion(gvk, allowDeprecated, allowExperimental); err != nil {
			return nil, err
		}

		// 校验 JSON/YAML 的有效性。
		if err := strict.VerifyUnmarshalStrict([]*runtime.Scheme{kubeadmscheme.Scheme, componentconfigs.Scheme}, gvk, fileContent); err != nil {
			if !strictErrors {
				klog.Warning(err.Error())
			} else {
				return nil, err
			}
		}

		if kubeadmutil.GroupVersionKindsHasInitConfiguration(gvk) {
			// 将 initcfg 置为空结构，供反序列化器填充。
			initcfg = &kubeadmapi.InitConfiguration{}
			// 将字节解码到内部结构；底层会先反序列化到正确的外部版本，再默认化并转换为内部版本。
			if err := runtime.DecodeInto(kubeadmscheme.Codecs.UniversalDecoder(), fileContent, initcfg); err != nil {
				return nil, err
			}
			continue
		}
		if kubeadmutil.GroupVersionKindsHasClusterConfiguration(gvk) {
			// 将 clustercfg 置为空结构，供反序列化器填充。
			clustercfg = &kubeadmapi.ClusterConfiguration{}
			// 将字节解码到内部结构；底层会先反序列化到正确的外部版本，再默认化并转换为内部版本。
			if err := runtime.DecodeInto(kubeadmscheme.Codecs.UniversalDecoder(), fileContent, clustercfg); err != nil {
				return nil, err
			}
			continue
		}

		// 若组既不是 kubeadm 核心类型也不是受支持的组件配置组，则警告其将被忽略。
		if !componentconfigs.Scheme.IsGroupRegistered(gvk.Group) {
			klog.Warningf("[config] WARNING: Ignored configuration document with GroupVersionKind %v\n", gvk)
		}
	}

	// 要求配置文档中至少存在 InitConfiguration 或 ClusterConfiguration。
	if initcfg == nil && clustercfg == nil {
		return nil, errors.New("no InitConfiguration or ClusterConfiguration kind was found in the configuration file")
	}

	// 未提供 InitConfiguration 时，创建外部结构、设置默认值并转换为内部类型。
	if initcfg == nil {
		extinitcfg := &kubeadmapiv1.InitConfiguration{}
		kubeadmscheme.Scheme.Default(extinitcfg)
		// 将 initcfg 置为空结构，供反序列化器填充。
		initcfg = &kubeadmapi.InitConfiguration{}
		if err := kubeadmscheme.Scheme.Convert(extinitcfg, initcfg, nil); err != nil {
			return nil, err
		}
	}
	// 若提供 ClusterConfiguration，将其填入 InitConfiguration。
	if clustercfg != nil {
		initcfg.ClusterConfiguration = *clustercfg
	} else {
		// 为内部 InitConfiguration.ClusterConfiguration 填充默认值。
		extclustercfg := &kubeadmapiv1.ClusterConfiguration{}
		kubeadmscheme.Scheme.Default(extclustercfg)
		if err := kubeadmscheme.Scheme.Convert(extclustercfg, &initcfg.ClusterConfiguration, nil); err != nil {
			return nil, err
		}
	}

	// 加载所有组件配置。
	if err := componentconfigs.FetchFromDocumentMap(&initcfg.ClusterConfiguration, gvkmap); err != nil {
		return nil, err
	}

	// 对参数未提供的设置应用动态默认值。
	if err := SetInitDynamicDefaults(initcfg, skipCRIDetect, false); err != nil {
		return nil, err
	}

	// 校验 cfg（参数/配置 + 默认值 + 动态默认值）。
	if err := validation.ValidateInitConfiguration(initcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return initcfg, nil
}

// MarshalInitConfigurationToBytes 将内部 InitConfiguration 序列化为字节。
// 内嵌 ClusterConfiguration 中的 ComponentConfigs 将作为独立 YAML/JSON 文档输出。
func MarshalInitConfigurationToBytes(cfg *kubeadmapi.InitConfiguration, gv schema.GroupVersion) ([]byte, error) {
	initbytes, err := kubeadmutil.MarshalToYamlForCodecs(cfg, gv, kubeadmscheme.Codecs)
	if err != nil {
		return []byte{}, err
	}
	allFiles := [][]byte{initbytes}

	// 例外：指定 GroupVersion 为内部类型时，不输出内嵌 ClusterConfiguration 内容。
	// 主要用于单元测试；实际场景不会直接按内部 API 版本序列化。
	if gv.Version != runtime.APIVersionInternal {
		clusterbytes, err := kubeadmutil.MarshalToYamlForCodecs(&cfg.ClusterConfiguration, gv, kubeadmscheme.Codecs)
		if err != nil {
			return []byte{}, err
		}
		allFiles = append(allFiles, clusterbytes)
	}
	return bytes.Join(allFiles, []byte(kubeadmconstants.YAMLDocumentSeparator)), nil
}
