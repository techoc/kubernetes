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

package load

import (
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	api "k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	externalapi "k8s.io/apiserver/pkg/apis/apiserver/v1alpha1"
)

var (
	// scheme/codecs 是包级共享的解码器。它们只读，构造后不再修改，
	// 因此可以被并发调用。
	scheme = runtime.NewScheme()
	// EnableStrict 让未知字段直接报错，避免配置拼写错误被静默忽略。
	codecs = serializer.NewCodecFactory(scheme, serializer.EnableStrict)
)

func init() {
	// 注册内部版本与所有外部版本（v1alpha1 等）及其默认值、转换函数。
	install.Install(scheme)
}

// LoadFromFile 从文件路径读取并解析 AuthorizationConfiguration。
func LoadFromFile(file string) (*api.AuthorizationConfiguration, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return LoadFromData(data)
}

// LoadFromReader 从 reader 读取并解析 AuthorizationConfiguration。
// reader 为 nil 表示未提供配置，此时返回默认配置。
func LoadFromReader(reader io.Reader) (*api.AuthorizationConfiguration, error) {
	if reader == nil {
		// 未指定 reader 时，使用默认配置
		return LoadFromData(nil)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return LoadFromData(data)
}

// LoadFromData 解析原始配置字节。
// 返回的始终是内部版本，便于调用方避免版本分支。
func LoadFromData(data []byte) (*api.AuthorizationConfiguration, error) {
	if len(data) == 0 {
		// 未提供配置时，返回默认配置
		// 先对空的外部对象应用默认值，再转换到内部版本，
		// 保证默认值只在外部类型上定义一处。
		externalConfig := &externalapi.AuthorizationConfiguration{}
		scheme.Default(externalConfig)
		internalConfig := &api.AuthorizationConfiguration{}
		if err := scheme.Convert(externalConfig, internalConfig, nil); err != nil {
			return nil, err
		}
		return internalConfig, nil
	}

	decodedObj, err := runtime.Decode(codecs.UniversalDecoder(), data)
	if err != nil {
		return nil, err
	}
	// 安装到 scheme 的配置类型必须都转换为内部版本，否则说明配置装错了类型。
	configuration, ok := decodedObj.(*api.AuthorizationConfiguration)
	if !ok {
		return nil, fmt.Errorf("expected AuthorizationConfiguration, got %T", decodedObj)
	}
	return configuration, nil
}
