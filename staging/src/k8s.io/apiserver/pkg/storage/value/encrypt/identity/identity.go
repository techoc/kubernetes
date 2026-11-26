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

// Package identity 提供不做任何转换的 value.Transformer，并在读取时拒绝已加密数据，
// 作为未启用静态加密时的安全兜底。
package identity

import (
	"bytes"
	"context"
	"fmt"

	"k8s.io/apiserver/pkg/storage/value"
)

// 单例转换器；该前缀是 etcd 中加密数据的保留标记，出现在非加密转换器读到的数据里说明配置错位。
var (
	transformer      = identityTransformer{}
	encryptedPrefix  = []byte("k8s:enc:")
	errEncryptedData = fmt.Errorf("identity transformer tried to read encrypted data")
)

// identityTransformer 不转换数据，但在 TransformFromStorage 时
// 校验数据不是密文
//
// 用途：作为"未配置加密"时的占位转换器，同时防止误把加密数据当作明文返回。
type identityTransformer struct{}

// NewEncryptCheckTransformer 返回一个 identityTransformer，读取密文时
// 会报错
func NewEncryptCheckTransformer() value.Transformer {
	return transformer
}

// 数据未加密时 TransformFromStorage 原样返回输入字节
func (identityTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	// 若数据由其他 transformer 编码，identityTransformer 必须报错。
	// JSON 数据以 '{' 开头，Protobuf 数据以 'k8s[\x00-\xFF]' 为前缀。
	// 前缀 'k8s:enc:' 专用于磁盘上的加密数据。
	if bytes.HasPrefix(data, encryptedPrefix) {
		return nil, false, errEncryptedData
	}
	return data, false, nil
}

// TransformToStorage 为 identityTransformer 实现 Transformer 接口
func (identityTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	return data, nil
}
