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

package options

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	configv1 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1"
)

// LoadConfigFromFile 从指定路径读取 Scheduler 组件配置，并交给统一解码流程处理。
func LoadConfigFromFile(logger klog.Logger, file string) (*config.KubeSchedulerConfiguration, error) {
	// 文件不存在、权限不足等 I/O 错误在这里直接返回，解码逻辑不处理文件系统细节。
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return loadConfig(data)
}

// loadConfig 将 YAML/JSON 配置解码为 Scheduler 内部版本，统一吸收外部 API 版本差异。
func loadConfig(data []byte) (*config.KubeSchedulerConfiguration, error) {
	// UniversalDecoder 根据 apiVersion/kind 选择外部版本，并执行默认值填充和版本转换；gvk 保留输入版本信息。
	obj, gvk, err := scheme.Codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	if cfgObj, ok := obj.(*config.KubeSchedulerConfiguration); ok {
		// 版本转换不会把外部 APIVersion 保存在内部对象 TypeMeta 中，API machinery 后续还会清理它。
		// 因此这里根据解码得到的 GVK 恢复版本号，供日志和配置回写流程识别原始配置版本。
		// API machinery 后续转换会清空该字段，详见 KubeSchedulerConfiguration 内部类型定义。
		cfgObj.TypeMeta.APIVersion = gvk.GroupVersion().String()
		return cfgObj, nil
	}
	return nil, fmt.Errorf("couldn't decode as KubeSchedulerConfiguration, got %s: ", gvk)
}

// encodeConfig 把内部配置重新编码为可读 YAML，供日志输出或 --write-config-to 使用。
func encodeConfig(cfg *config.KubeSchedulerConfiguration) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	// 对外固定输出 YAML，版本编码器负责把内部对象转换为公开 API 表示。
	const mediaType = runtime.ContentTypeYAML
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return buf, fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}

	var encoder runtime.Encoder
	switch cfg.TypeMeta.APIVersion {
	case configv1.SchemeGroupVersion.String():
		encoder = scheme.Codecs.EncoderForVersion(info.Serializer, configv1.SchemeGroupVersion)
	default:
		encoder = scheme.Codecs.EncoderForVersion(info.Serializer, configv1.SchemeGroupVersion)
	}
	if err := encoder.Encode(cfg, buf); err != nil {
		return buf, err
	}
	return buf, nil
}

// LogOrWriteConfig 记录实例化完成的组件配置，并在启用导出时以 YAML 写入指定文件。
func LogOrWriteConfig(logger klog.Logger, fileName string, cfg *config.KubeSchedulerConfiguration, completedProfiles []config.KubeSchedulerProfile) error {
	loggerV := logger.V(2)
	// 既不要求写文件、日志级别也不够高时，无需执行序列化。
	if !loggerV.Enabled() && len(fileName) == 0 {
		return nil
	}
	// 使用框架实例化后的 Profile，确保导出结果包含默认插件展开后的真实配置。
	cfg.Profiles = completedProfiles

	buf, err := encodeConfig(cfg)
	if err != nil {
		return err
	}

	if loggerV.Enabled() {
		loggerV.Info("Using component config", "config", buf.String())
	}

	// --write-config-to 是一次性导出模式：配置写入完成后直接成功退出，不启动调度循环。
	if len(fileName) > 0 {
		configFile, err := os.Create(fileName)
		if err != nil {
			return err
		}
		defer configFile.Close()
		if _, err := io.Copy(configFile, buf); err != nil {
			return err
		}
		logger.Info("Wrote configuration", "file", fileName)
		os.Exit(0)
	}
	return nil
}
