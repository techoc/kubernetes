/*
Copyright 2021 The Kubernetes Authors.

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

package load

// 本包集中处理配置文件的读取与解码，避免各调用方各自拼接序列化逻辑。

import (
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/pod-security-admission/admission/api"
	"k8s.io/pod-security-admission/admission/api/scheme"
	apiv1 "k8s.io/pod-security-admission/admission/api/v1"
)

// 路径为空等同于没有配置：直接生成默认配置，而不是把空路径当文件读。
func LoadFromFile(file string) (*api.PodSecurityConfiguration, error) {
	if len(file) == 0 {
		// no file specified, use default config
		return LoadFromData(nil)
	}

	// read from file
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return LoadFromData(data)
}

// 与 LoadFromFile 同理，nil reader 表示未指定输入，回落到默认配置。
func LoadFromReader(reader io.Reader) (*api.PodSecurityConfiguration, error) {
	if reader == nil {
		// no reader specified, use default config
		return LoadFromData(nil)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return LoadFromData(data)
}

// 空数据先经过 scheme 默认值再转内部类型，保证缺省字段与显式声明默认值结果一致。
func LoadFromData(data []byte) (*api.PodSecurityConfiguration, error) {
	if len(data) == 0 {
		// no config provided, return default
		externalConfig := &apiv1.PodSecurityConfiguration{}
		scheme.Scheme.Default(externalConfig)
		internalConfig := &api.PodSecurityConfiguration{}
		if err := scheme.Scheme.Convert(externalConfig, internalConfig, nil); err != nil {
			return nil, err
		}
		return internalConfig, nil
	}

	// 统一解码器同时处理版本化和默认化，调用方无需关心配置写在哪个 apiVersion。
	decodedObj, err := runtime.Decode(scheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		return nil, err
	}
	configuration, ok := decodedObj.(*api.PodSecurityConfiguration)
	if !ok {
		return nil, fmt.Errorf("expected PodSecurityConfiguration, got %T", decodedObj)
	}
	return configuration, nil
}
