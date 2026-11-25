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

package admission

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
)

// makeAbs 把 path 解析为绝对路径。相对路径以 base 为基准；base 为空或 "."
// 时退回到进程当前工作目录。插件配置中的相对路径必须相对于配置文件所在目录，
// 而不是启动 apiserver 的目录，因此调用方会传入配置文件所在目录作为 base。
func makeAbs(path, base string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	if len(base) == 0 || base == "." {
		cwd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		base = cwd
	}
	return filepath.Join(base, path), nil
}

// ReadAdmissionConfiguration 读取指定路径的准入配置。
// 若输入文件符合要求的语法，返回加载后的准入配置；若不符合，则为所枚举的
// pluginNames 集合生成引用该 configFilePath 的默认配置——这是为了保留
// 准入控制文件尚为不透明格式时的向后兼容性。文件不存在时返回错误。
//
// 该函数同时兼容新旧两种配置文件格式：新格式是版本化的 AdmissionConfiguration；
// 旧格式是 ImagePolicyWebhook / PodNodeSelector 插件的裸配置。旧格式无法被
// UniversalDecoder 识别，遇到时会被转换为等价的 AdmissionConfiguration，
// 以便老集群平滑升级。
func ReadAdmissionConfiguration(pluginNames []string, configFilePath string, configScheme *runtime.Scheme) (ConfigProvider, error) {
	if configFilePath == "" {
		// 未指定配置文件：返回空配置，调用方会按插件默认值运行。
		return configProvider{config: &apiserver.AdmissionConfiguration{}}, nil
	}
	// 提供了配置文件，直接读取。
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read admission control configuration from %q [%v]", configFilePath, err)
	}
	// 使用严格模式解码，避免未知字段被静默丢弃而掩盖配置错误。
	codecs := serializer.NewCodecFactory(configScheme, serializer.EnableStrict)
	decoder := codecs.UniversalDecoder()
	decodedObj, err := runtime.Decode(decoder, data)
	// 文件成功解码
	if err == nil {
		decodedConfig, ok := decodedObj.(*apiserver.AdmissionConfiguration)
		if !ok {
			return nil, fmt.Errorf("unexpected type: %T", decodedObj)
		}
		// 插件配置文件的相对路径以本配置文件所在目录为基准，统一转换为绝对路径，
		// 这样后续无论进程工作目录如何变化都能找到文件。
		baseDir := path.Dir(configFilePath)
		for i := range decodedConfig.Plugins {
			if decodedConfig.Plugins[i].Path == "" {
				continue
			}
			// 把相对路径转换为绝对路径
			absPath, err := makeAbs(decodedConfig.Plugins[i].Path, baseDir)
			if err != nil {
				return nil, err
			}
			decodedConfig.Plugins[i].Path = absPath
		}
		return configProvider{
			config: decodedConfig,
		}, nil
	}
	// 只有“缺少 apiVersion/kind”或“类型未注册”这类错误才可能意味着这是旧格式
	// 裸配置；其它错误（例如语法错误）必须原样返回，不能当作旧格式吞掉。
	// 解码报错，且并非缺少版本/类型未注册这类错误
	if !(runtime.IsMissingVersion(err) || runtime.IsMissingKind(err) || runtime.IsNotRegisteredError(err)) {
		return nil, err
	}

	// 通过顶层 key 判断是否为旧格式；不是则把原始解码错误返回，避免掩盖真正的问题。
	unstructuredData := map[string]interface{}{}
	if err2 := yaml.Unmarshal(data, &unstructuredData); err2 != nil {
		return nil, err
	}
	_, isLegacyImagePolicy := unstructuredData["imagePolicy"]
	_, isLegacyPodNodeSelector := unstructuredData["podNodeSelectorPluginConfig"]
	if !isLegacyImagePolicy && !isLegacyPodNodeSelector {
		return nil, err
	}

	// 把旧格式转换为新的准入控制格式：为保留向后兼容性，将过去从未版本化
	// 文件配置读取输入的插件指向当前输入文件。
	// 只有这两个插件历史上使用未版本化的裸配置；其余插件不受此兼容逻辑影响。
	legacyPluginsWithUnversionedConfig := sets.New[string]("ImagePolicyWebhook", "PodNodeSelector")
	externalConfig := &apiserverv1.AdmissionConfiguration{}
	for _, pluginName := range pluginNames {
		if legacyPluginsWithUnversionedConfig.Has(pluginName) {
			// 把旧文件本身登记为插件的 Path，读取时插件会再次解析该文件。
			externalConfig.Plugins = append(externalConfig.Plugins,
				apiserverv1.AdmissionPluginConfiguration{
					Name: pluginName,
					Path: configFilePath})
		}
	}
	// 先对 v1 外部版本做 defaulting，再转换为内部版本，保证默认值与正常解码路径一致。
	configScheme.Default(externalConfig)
	internalConfig := &apiserver.AdmissionConfiguration{}
	if err := configScheme.Convert(externalConfig, internalConfig, nil); err != nil {
		return nil, err
	}
	return configProvider{
		config: internalConfig,
	}, nil
}

// configProvider 是 ConfigProvider 的简单实现，只持有一份解析好的内部配置。
type configProvider struct {
	config *apiserver.AdmissionConfiguration
}

// GetAdmissionPluginConfigurationFor 返回持有准入插件配置的 reader。
//
// 返回的 reader 有两种来源：内嵌的 Configuration.Raw（优先）或 Path 指向的文件。
// 两者都没有时返回 (nil, nil)，表示该插件没有额外配置。
func GetAdmissionPluginConfigurationFor(pluginCfg apiserver.AdmissionPluginConfiguration) (io.Reader, error) {
	// 若存在内嵌对象，直接返回它
	if pluginCfg.Configuration != nil {
		return bytes.NewBuffer(pluginCfg.Configuration.Raw), nil
	}
	// 配置文件在启动阶段读取；读不到说明部署有误，直接 Fatal 终止，避免带病运行。
	if pluginCfg.Path != "" {
		content, err := os.ReadFile(pluginCfg.Path)
		if err != nil {
			klog.Fatalf("Couldn't open admission plugin configuration %s: %#v", pluginCfg.Path, err)
			return nil, err
		}
		return bytes.NewBuffer(content), nil
	}
	// 完全没有特殊配置
	return nil, nil
}

// ConfigFor 返回指定插件的 reader；若无对应配置则返回 nil reader。
//
// 按插件名线性查找；插件名必须与注册名完全一致，不做大小写或别名归一化。
func (p configProvider) ConfigFor(pluginName string) (io.Reader, error) {
	// 没有任何配置，自然也没有可用配置
	if p.config == nil {
		return nil, nil
	}
	// 查找同名插件并取其配置
	for _, pluginCfg := range p.config.Plugins {
		if pluginName != pluginCfg.Name {
			continue
		}
		pluginConfig, err := GetAdmissionPluginConfigurationFor(pluginCfg)
		if err != nil {
			return nil, err
		}
		return pluginConfig, nil
	}
	// 没有按插件名匹配上的已注册配置。
	return nil, nil
}
