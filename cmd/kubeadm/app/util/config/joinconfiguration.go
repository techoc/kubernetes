/*
Copyright 2018 The Kubernetes Authors.

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

// SetJoinDynamicDefaults 检查并设置 JoinConfiguration 的配置值。
func SetJoinDynamicDefaults(cfg *kubeadmapi.JoinConfiguration, skipCRIDetect bool) error {
	addControlPlaneTaint := false
	if cfg.ControlPlane != nil {
		addControlPlaneTaint = true
	}
	if err := SetNodeRegistrationDynamicDefaults(&cfg.NodeRegistration, addControlPlaneTaint, skipCRIDetect); err != nil {
		return err
	}

	return SetJoinControlPlaneDefaults(cfg.ControlPlane)
}

// SetJoinControlPlaneDefaults 检查并设置 JoinControlPlane 的配置值。
func SetJoinControlPlaneDefaults(cfg *kubeadmapi.JoinControlPlane) error {
	if cfg != nil {
		if err := SetAPIEndpointDynamicDefaults(&cfg.LocalAPIEndpoint); err != nil {
			return err
		}
	}
	return nil
}

// LoadOrDefaultJoinConfiguration 接收配置文件路径和可作为默认值的版本化配置。
// 指定 cfgPath 时始终以文件覆盖 defaultversionedcfg，否则使用默认配置（通常由参数填充）。
// 随后将外部版本化配置默认化并转换为内部类型，再应用本机 IP 等动态默认值，最后校验返回。
func LoadOrDefaultJoinConfiguration(cfgPath string, defaultversionedcfg *kubeadmapiv1.JoinConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.JoinConfiguration, error) {
	var (
		config *kubeadmapi.JoinConfiguration
		err    error
	)
	if cfgPath != "" {
		// 若提供了配置文件，从中加载配置。
		config, err = LoadJoinConfigurationFromFile(cfgPath, opts)
	} else {
		config, err = DefaultedJoinConfiguration(defaultversionedcfg, opts)
	}
	if err == nil {
		prepareStaticVariables(config)
	}
	return config, err
}

// LoadJoinConfigurationFromFile 从文件加载版本化 JoinConfiguration，转换为内部类型、默认化并校验。
func LoadJoinConfigurationFromFile(cfgPath string, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.JoinConfiguration, error) {
	klog.V(1).Infof("loading configuration from %q", cfgPath)

	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read config from %q ", cfgPath)
	}

	return BytesToJoinConfiguration(b, opts)
}

// BytesToJoinConfiguration 将字节切片转换为已默认化并校验的内部 JoinConfiguration。
// 输入可包含多个 YAML/JSON 文档，逐个解析；返回前应用动态默认值并校验。
func BytesToJoinConfiguration(b []byte, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.JoinConfiguration, error) {
	// 将文件中的 YAML/JSON 文档拆分为 DocumentMap。
	gvkmap, err := kubeadmutil.SplitConfigDocuments(b)
	if err != nil {
		return nil, err
	}

	return documentMapToJoinConfiguration(gvkmap, false, false, false, opts.SkipCRIDetect)
}

// documentMapToJoinConfiguration 接收 SplitYAMLDocuments 返回的 GVK 与 YAML/JSON 文档映射，
// 查找 JoinConfiguration，解码、动态默认化并校验后返回。
func documentMapToJoinConfiguration(gvkmap kubeadmapi.DocumentMap, allowDeprecated, allowExperimental, strictErrors, skipCRIDetect bool) (*kubeadmapi.JoinConfiguration, error) {
	joinBytes := []byte{}
	for gvk, bytes := range gvkmap {
		// 仅处理 JoinConfiguration，忽略其他类型。
		if gvk.Kind != constants.JoinConfigurationKind {
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

		joinBytes = bytes
	}

	if len(joinBytes) == 0 {
		return nil, errors.Errorf("no %s found in the supplied config", constants.JoinConfigurationKind)
	}

	internalcfg := &kubeadmapi.JoinConfiguration{}
	if err := runtime.DecodeInto(kubeadmscheme.Codecs.UniversalDecoder(), joinBytes, internalcfg); err != nil {
		return nil, err
	}

	// 对参数未提供的设置应用动态默认值。
	if err := SetJoinDynamicDefaults(internalcfg, skipCRIDetect); err != nil {
		return nil, err
	}
	// 校验 cfg（参数/配置 + 默认值）。
	if err := validation.ValidateJoinConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return internalcfg, nil
}

// DefaultedJoinConfiguration 接收通常由命令行参数填充的版本化 JoinConfiguration，
// 默认化、转换为内部类型并校验。
func DefaultedJoinConfiguration(defaultversionedcfg *kubeadmapiv1.JoinConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.JoinConfiguration, error) {
	internalcfg := &kubeadmapi.JoinConfiguration{}

	// 考虑已传入的参数；再次执行默认值设置，仅对参数未提供的 cfg 字段赋静态默认值。
	kubeadmscheme.Scheme.Default(defaultversionedcfg)
	if err := kubeadmscheme.Scheme.Convert(defaultversionedcfg, internalcfg, nil); err != nil {
		return nil, err
	}

	// 对参数未提供的设置应用动态默认值。
	if err := SetJoinDynamicDefaults(internalcfg, opts.SkipCRIDetect); err != nil {
		return nil, err
	}
	// 校验 cfg（参数/配置 + 默认值）。
	if err := validation.ValidateJoinConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return internalcfg, nil
}
