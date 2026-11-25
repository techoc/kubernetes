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

package config

import (
	"os"
	"strings"

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
	kubeadmruntime "k8s.io/kubernetes/cmd/kubeadm/app/util/runtime"
)

// SetResetDynamicDefaults 检查并设置 ResetConfiguration 的配置值。
func SetResetDynamicDefaults(cfg *kubeadmapi.ResetConfiguration, skipCRIDetect bool) error {
	var err error
	if cfg.CRISocket == "" {
		if skipCRIDetect {
			klog.V(4).Infof("skip CRI socket detection, fill with the default CRI socket %s", constants.DefaultCRISocket)
			cfg.CRISocket = constants.DefaultCRISocket
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

// LoadOrDefaultResetConfiguration 接收配置文件路径和可作为默认值的版本化配置。
// 指定 cfgPath 时始终以文件覆盖 defaultversionedcfg，否则使用默认配置（通常由参数填充）。
// 随后将外部版本化配置默认化并转换为内部类型，再应用动态默认值，最后校验返回。
func LoadOrDefaultResetConfiguration(cfgPath string, defaultversionedcfg *kubeadmapiv1.ResetConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.ResetConfiguration, error) {
	var (
		config *kubeadmapi.ResetConfiguration
		err    error
	)
	if cfgPath != "" {
		// 若提供了配置文件，从中加载配置。
		config, err = LoadResetConfigurationFromFile(cfgPath, opts)
	} else {
		config, err = DefaultedResetConfiguration(defaultversionedcfg, opts)
	}
	if err == nil {
		prepareStaticVariables(config)
	}
	return config, err
}

// LoadResetConfigurationFromFile 从文件加载版本化 ResetConfiguration，转换为内部类型、默认化并校验。
func LoadResetConfigurationFromFile(cfgPath string, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.ResetConfiguration, error) {
	klog.V(1).Infof("loading configuration from %q", cfgPath)

	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read config from %q ", cfgPath)
	}

	return BytesToResetConfiguration(b, opts)
}

// BytesToResetConfiguration 将字节切片转换为已默认化并校验的内部 ResetConfiguration。
// 输入可包含多个 YAML/JSON 文档，逐个解析；返回前应用动态默认值并校验。
func BytesToResetConfiguration(b []byte, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.ResetConfiguration, error) {
	// 将文件中的 YAML/JSON 文档拆分为 DocumentMap。
	gvkmap, err := kubeadmutil.SplitConfigDocuments(b)
	if err != nil {
		return nil, err
	}

	return documentMapToResetConfiguration(gvkmap, false, opts.AllowExperimental, false, opts.SkipCRIDetect)
}

// documentMapToResetConfiguration 接收 SplitYAMLDocuments 返回的 GVK 与 YAML/JSON 文档映射，
// 查找 ResetConfiguration，解码、动态默认化并校验后返回。
func documentMapToResetConfiguration(gvkmap kubeadmapi.DocumentMap, allowDeprecated, allowExperimental bool, strictErrors bool, skipCRIDetect bool) (*kubeadmapi.ResetConfiguration, error) {
	resetBytes := []byte{}
	for gvk, bytes := range gvkmap {
		// 仅处理 ResetConfiguration，忽略其他类型。
		if gvk.Kind != constants.ResetConfigurationKind {
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

		resetBytes = bytes
	}

	if len(resetBytes) == 0 {
		return nil, errors.Errorf("no %s found in the supplied config", constants.ResetConfigurationKind)
	}

	internalcfg := &kubeadmapi.ResetConfiguration{}
	if err := runtime.DecodeInto(kubeadmscheme.Codecs.UniversalDecoder(), resetBytes, internalcfg); err != nil {
		return nil, err
	}

	// 对参数未提供的设置应用动态默认值。
	if err := SetResetDynamicDefaults(internalcfg, skipCRIDetect); err != nil {
		return nil, err
	}
	// 校验 cfg。
	if err := validation.ValidateResetConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return internalcfg, nil
}

// DefaultedResetConfiguration 接收通常由命令行参数填充的版本化 ResetConfiguration，
// 默认化、转换为内部类型并校验。
func DefaultedResetConfiguration(defaultversionedcfg *kubeadmapiv1.ResetConfiguration, opts LoadOrDefaultConfigurationOptions) (*kubeadmapi.ResetConfiguration, error) {
	internalcfg := &kubeadmapi.ResetConfiguration{}

	// 考虑已传入的参数；再次执行默认值设置，仅对参数未提供的 cfg 字段赋静态默认值。
	kubeadmscheme.Scheme.Default(defaultversionedcfg)
	if err := kubeadmscheme.Scheme.Convert(defaultversionedcfg, internalcfg, nil); err != nil {
		return nil, err
	}

	// 对参数未提供的设置应用动态默认值。
	if err := SetResetDynamicDefaults(internalcfg, opts.SkipCRIDetect); err != nil {
		return nil, err
	}
	// 校验 cfg。
	if err := validation.ValidateResetConfiguration(internalcfg).ToAggregate(); err != nil {
		return nil, err
	}

	return internalcfg, nil
}
