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

package resourcequota

import (
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	resourcequotaapi "k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota"
	"k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota/install"
	resourcequotav1 "k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota/v1"
)

var (
	// 独立 scheme 仅服务于配置文件解码，不与全局 scheme 混用，避免注册冲突。
	scheme = runtime.NewScheme()
	// EnableStrict 让未知字段直接报错，防止配置拼写错误被静默忽略。
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)
)

func init() {
	// 注册内部类型及 v1/v1beta1 与内部版本之间的转换。
	install.Install(scheme)
}

// 解析 ResourceQuota 准入控制器的配置文件；返回的是内部版本，调用方无需
// 关心外部版本差异。nil 输入被视为“未配置”，返回默认配置。
func LoadConfiguration(config io.Reader) (*resourcequotaapi.Configuration, error) {
	// 没有提供配置时返回默认配置：借助外部版本构造再转换，保证与真实解码路径共用同一套默认值逻辑。
	if config == nil {
		externalConfig := &resourcequotav1.Configuration{}
		scheme.Default(externalConfig)
		internalConfig := &resourcequotaapi.Configuration{}
		if err := scheme.Convert(externalConfig, internalConfig, nil); err != nil {
			return nil, err
		}
		return internalConfig, nil
	}
	// we have a config so parse it.
	data, err := io.ReadAll(config)
	if err != nil {
		return nil, err
	}
	// UniversalDecoder 依据 apiVersion/kind 选择版本并转换到内部类型。
	decoder := codecs.UniversalDecoder()
	decodedObj, err := runtime.Decode(decoder, data)
	if err != nil {
		return nil, err
	}
	// 解码结果应为内部 Configuration；类型不符说明配置写错了 kind。
	resourceQuotaConfiguration, ok := decodedObj.(*resourcequotaapi.Configuration)
	if !ok {
		return nil, fmt.Errorf("unexpected type: %T", decodedObj)
	}
	return resourceQuotaConfiguration, nil
}
