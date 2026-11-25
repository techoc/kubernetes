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

// LoadFromFile 读取并解码配置文件；空路径表示未指定配置，返回默认配置。
func LoadFromFile(file string) (*api.PodSecurityConfiguration, error) {
	if len(file) == 0 {
		// 未指定文件时使用默认配置。
		return LoadFromData(nil)
	}

	// 读取文件内容后统一交给数据加载逻辑。
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return LoadFromData(data)
}

// LoadFromReader 读取并解码配置流；nil reader 表示未指定输入，返回默认配置。
func LoadFromReader(reader io.Reader) (*api.PodSecurityConfiguration, error) {
	if reader == nil {
		// 未指定 reader 时使用默认配置。
		return LoadFromData(nil)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return LoadFromData(data)
}

// LoadFromData 解码配置字节；空数据先应用默认值，非空数据严格解码后转内部类型，
// 保证缺省字段与显式配置得到一致的规范化结果。
func LoadFromData(data []byte) (*api.PodSecurityConfiguration, error) {
	if len(data) == 0 {
		// 未提供配置数据时返回默认配置。
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
