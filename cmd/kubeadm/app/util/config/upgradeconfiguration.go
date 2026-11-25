/*
Copyright 2024 The Kubernetes Authors.

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
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/config/strict"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// documentMapToUpgradeConfiguration 接收 SplitYAMLDocuments 返回的 GVK 与 YAML/JSON 文档映射，
// 查找 UpgradeConfiguration，解码、动态默认化并校验后返回。
func documentMapToUpgradeConfiguration(gvkmap kubeadmapi.DocumentMap, allowDeprecated, allowExperimental, strictErrors bool) (*kubeadmapi.UpgradeConfiguration, error) {
	upgradeBytes := []byte{}

	for gvk, bytes := range gvkmap {
		if gvk.Kind != constants.UpgradeConfigurationKind {
			klog.Warningf("[config] WARNING: Ignored configuration document with GroupVersionKind %v\n", gvk)
			continue
		}

		// 检查版本是否受支持且未弃用。
		if err := validateSupportedVersion(gvk, allowDeprecated, allowExperimental); err != nil {
			return nil, err
		}

		// 校验 YAML/JSON 的有效性。
		if err := strict.VerifyUnmarshalStrict([]*runtime.Scheme{kubeadmscheme.Scheme}, gvk, bytes); err != nil {
			if !strictErrors {
				klog.Warning(err.Error())
			} else {
				return nil, err
			}
		}

		upgradeBytes = bytes
	}

	if len(upgradeBytes) == 0 {
		return nil, errors.Errorf("no %s found in the supplied config", constants.UpgradeConfigurationKind)
	}

	// 将 internalcfg 置为空结构，供反序列化器填充。
	internalcfg := &kubeadmapi.UpgradeConfiguration{}
	// 将字节解码到内部结构；底层会先反序列化到正确的外部版本，再默认化并转换为内部版本。
	if err := runtime.DecodeInto(kubeadmscheme.Codecs.UniversalDecoder(), upgradeBytes, internalcfg); err != nil {
		return nil, err
	}

	// 校验 cfg。
	if err := validation.ValidateUpgradeConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return internalcfg, nil
}

// LoadUpgradeConfigurationFromFile 从文件加载 UpgradeConfiguration。
func LoadUpgradeConfigurationFromFile(cfgPath string, _ LoadOrDefaultConfigurationOptions) (*kubeadmapi.UpgradeConfiguration, error) {
	var err error
	var upgradeCfg *kubeadmapi.UpgradeConfiguration

	// 否则加载指定的配置文件。
	configBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load config from file %q", cfgPath)
	}

	// 将 documentMap 转换为内部 UpgradeConfiguration；忽略文件中的 InitConfiguration 和
	// ClusterConfiguration。升级应沿用现有集群配置，不允许通过配置文件重新配置集群。
	if upgradeCfg, err = BytesToUpgradeConfiguration(configBytes); err != nil {
		return nil, err
	}

	return upgradeCfg, nil
}

// BytesToUpgradeConfiguration 将字节切片转换为已默认化并校验的内部 UpgradeConfiguration。
// 输入可包含多个 YAML/JSON 文档，逐个解析；返回前应用动态默认值并校验。
func BytesToUpgradeConfiguration(b []byte) (*kubeadmapi.UpgradeConfiguration, error) {
	// 将文件中的 YAML/JSON 文档拆分为 DocumentMap。
	gvkmap, err := kubeadmutil.SplitConfigDocuments(b)
	if err != nil {
		return nil, err
	}

	return documentMapToUpgradeConfiguration(gvkmap, false, false, false)
}

// LoadOrDefaultUpgradeConfiguration 接收配置文件路径和可作为默认值的版本化配置。
// 指定 cfgPath 时始终以文件覆盖 defaultversionedcfg，否则使用默认配置（通常由参数填充）。
// 随后将外部版本化配置默认化并转换为内部类型，再应用动态默认值，最后校验返回。
func LoadOrDefaultUpgradeConfiguration(cfgPath string, defaultversionedcfg *kubeadmapiv1.UpgradeConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.UpgradeConfiguration, error) {
	var (
		config *kubeadmapi.UpgradeConfiguration
		err    error
	)
	if cfgPath != "" {
		// 若提供了配置文件，从中加载配置。
		config, err = LoadUpgradeConfigurationFromFile(cfgPath, opts)
	} else {
		config, err = DefaultedUpgradeConfiguration(defaultversionedcfg, opts)
	}
	if err == nil {
		prepareStaticVariables(config)
	}
	return config, err
}

// DefaultedUpgradeConfiguration 接收通常由命令行参数填充的版本化 UpgradeConfiguration，
// 默认化、转换为内部类型并校验。
func DefaultedUpgradeConfiguration(defaultversionedcfg *kubeadmapiv1.UpgradeConfiguration, _ LoadOrDefaultConfigurationOptions) (*kubeadmapi.UpgradeConfiguration, error) {
	internalcfg := &kubeadmapi.UpgradeConfiguration{}

	// 考虑已传入的参数；再次执行默认值设置，仅对参数未提供的 cfg 字段赋静态默认值。
	kubeadmscheme.Scheme.Default(defaultversionedcfg)
	if err := kubeadmscheme.Scheme.Convert(defaultversionedcfg, internalcfg, nil); err != nil {
		return nil, err
	}

	// 校验 cfg。
	if err := validation.ValidateUpgradeConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return internalcfg, nil
}
