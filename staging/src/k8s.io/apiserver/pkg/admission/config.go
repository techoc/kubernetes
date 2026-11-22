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

// ReadAdmissionConfiguration reads the admission configuration at the specified path.
// It returns the loaded admission configuration if the input file aligns with the required syntax.
// If it does not align with the provided syntax, it returns a default configuration for the enumerated
// set of pluginNames whose config location references the specified configFilePath.
// It does this to preserve backward compatibility when admission control files were opaque.
// It returns an error if the file did not exist.
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
	// a file was provided, so we just read it.
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read admission control configuration from %q [%v]", configFilePath, err)
	}
	// 使用严格模式解码，避免未知字段被静默丢弃而掩盖配置错误。
	codecs := serializer.NewCodecFactory(configScheme, serializer.EnableStrict)
	decoder := codecs.UniversalDecoder()
	decodedObj, err := runtime.Decode(decoder, data)
	// we were able to decode the file successfully
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
			// we update relative file paths to absolute paths
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
	// we got an error where the decode wasn't related to a missing type
	if !(runtime.IsMissingVersion(err) || runtime.IsMissingKind(err) || runtime.IsNotRegisteredError(err)) {
		return nil, err
	}

	// Only tolerate load errors if the file appears to be one of the two legacy plugin configs
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

	// convert the legacy format to the new admission control format
	// in order to preserve backwards compatibility, we set plugins that
	// previously read input from a non-versioned file configuration to the
	// current input file.
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

// GetAdmissionPluginConfigurationFor returns a reader that holds the admission plugin configuration.
//
// 返回的 reader 有两种来源：内嵌的 Configuration.Raw（优先）或 Path 指向的文件。
// 两者都没有时返回 (nil, nil)，表示该插件没有额外配置。
func GetAdmissionPluginConfigurationFor(pluginCfg apiserver.AdmissionPluginConfiguration) (io.Reader, error) {
	// if there is a nest object, return it directly
	if pluginCfg.Configuration != nil {
		return bytes.NewBuffer(pluginCfg.Configuration.Raw), nil
	}
	// there is nothing nested, so we delegate to path
	// 配置文件在启动阶段读取；读不到说明部署有误，直接 Fatal 终止，避免带病运行。
	if pluginCfg.Path != "" {
		content, err := os.ReadFile(pluginCfg.Path)
		if err != nil {
			klog.Fatalf("Couldn't open admission plugin configuration %s: %#v", pluginCfg.Path, err)
			return nil, err
		}
		return bytes.NewBuffer(content), nil
	}
	// there is no special config at all
	return nil, nil
}

// ConfigFor returns a reader for the specified plugin.
// If no specific configuration is present, we return a nil reader.
//
// 按插件名线性查找；插件名必须与注册名完全一致，不做大小写或别名归一化。
func (p configProvider) ConfigFor(pluginName string) (io.Reader, error) {
	// there is no config, so there is no potential config
	if p.config == nil {
		return nil, nil
	}
	// look for matching plugin and get configuration
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
	// there is no registered config that matches on plugin name.
	return nil, nil
}
